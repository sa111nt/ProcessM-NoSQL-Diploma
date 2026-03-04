package pql.executor

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import db.CouchDBManager
import pql.model.PqlCondition
import pql.model.PqlQuery
import pql.translator.PqlFieldMapper
import pql.translator.PqlToCouchDbTranslator
import pql.model.PqlScope
import pql.model.SortDirection
import java.time.OffsetDateTime
import java.time.format.DateTimeParseException

/**
 * Główny koordynator wykonywania zapytań PQL.
 * * Klasa odpowiada za pełny cykl życia zapytania:
 * 1. Tłumaczenie struktury PqlQuery na natywne zapytania Mango JSON (CouchDB).
 * 2. Rozwiązywanie relacji między logami, śladami i zdarzeniami (Cross-Scope).
 * 3. Wykonywanie zapytań w bazie oraz pobieranie brakujących dokumentów nadrzędnych.
 * 4. Pamięciowe grupowanie (Process Mining & SQL) oraz wyliczanie agregacji/matematyki.
 * 5. Formatowanie końcowych wyników dla klienta.
 */
class PqlQueryExecutor(
    private val dbManager: CouchDBManager,
    private val dbName: String
) {
    private val translator = PqlToCouchDbTranslator()
    private val fieldMapper = PqlFieldMapper()
    private val formatter = PqlResultFormatter(fieldMapper)

    /**
     * Główna metoda wykonująca zapytanie.
     * * @param query Sparsowany obiekt zapytania PQL.
     * @return JsonArray zawierający sformatowane wyniki spełniające warunki zapytania.
     */
    fun execute(query: PqlQuery): JsonArray {
        if (query.conditions.any { it is PqlCondition.AlwaysFalse }) {
            return JsonArray()
        }

        // --- 1. MOST RELACYJNY (Cross-Scope) ---
        val crossScopeConditions = query.conditions.filter { getCrossScope(it, query.collection) != null }
        val sameScopeConditions = query.conditions.filter { getCrossScope(it, query.collection) == null }
        var resolvedQuery = query

        if (crossScopeConditions.isNotEmpty()) {
            val outerScope = crossScopeConditions.firstNotNullOfOrNull { getCrossScope(it, query.collection) }!!
            val parentIds = resolveCrossScopeConditions(crossScopeConditions, query.collection)

            if (parentIds.isEmpty()) return JsonArray()

            val filterField = if (outerScope == PqlScope.LOG && query.collection == PqlScope.EVENT) "traceId"
            else if (outerScope == PqlScope.LOG) "logId" else "traceId"

            val idCondition = if (parentIds.size == 1) {
                PqlCondition.Simple(query.collection, filterField, pql.model.PqlOperator.EQ, parentIds.first())
            } else {
                PqlCondition.Simple(query.collection, filterField, pql.model.PqlOperator.IN, parentIds)
            }
            resolvedQuery = query.copy(conditions = sameScopeConditions + idCondition)
        }

        val hasCrossScopeOrderBy = resolvedQuery.orderBy.any { it.scope != resolvedQuery.collection }
        val mangoQuery: JsonObject = translator.translate(resolvedQuery)

        // Usuwamy limity dla CouchDB, jeśli operacje wymagają pamięciowej obróbki
        if (hasCrossScopeOrderBy || resolvedQuery.groupBy.isNotEmpty()) {
            mangoQuery.remove("limit")
            mangoQuery.remove("skip")
            mangoQuery.remove("sort")
            mangoQuery.addProperty("limit", 100000)
        }

        if (resolvedQuery.orderBy.isNotEmpty() && !hasCrossScopeOrderBy && resolvedQuery.groupBy.isEmpty()) {
            val indexFields = mutableListOf("docType")
            for (order in resolvedQuery.orderBy) {
                indexFields.add(fieldMapper.resolve(order.scope, order.attribute).toDotPath())
            }
            dbManager.ensureIndex(dbName, indexFields)
        }

        var rawResult: JsonArray = dbManager.findDocs(dbName, mangoQuery)

        // --- 2. OPERACJE PAMIĘCIOWE ---
        if (hasCrossScopeOrderBy && rawResult.size() > 0 && resolvedQuery.groupBy.isEmpty()) {
            rawResult = executeCrossScopeOrderBy(rawResult, resolvedQuery)
        }

        if (resolvedQuery.groupBy.isNotEmpty()) {
            return formatGroupedResponse(rawResult, resolvedQuery)
        }

        return formatter.format(rawResult, resolvedQuery)
    }

    /**
     * Sortuje wyniki w pamięci na podstawie atrybutów należących do nadrzędnego Scope'u
     * (np. sortowanie zdarzeń po koszcie całego śladu t:cost).
     */
    private fun executeCrossScopeOrderBy(rawResult: JsonArray, query: PqlQuery): JsonArray {
        val traceIds = mutableSetOf<String>()
        val logIds = mutableSetOf<String>()

        val needsTrace = query.orderBy.any { it.scope == PqlScope.TRACE }
        val needsLog = query.orderBy.any { it.scope == PqlScope.LOG }

        if (needsTrace || needsLog) {
            rawResult.forEach { doc ->
                val obj = doc.asJsonObject
                if (needsTrace) obj.get("traceId")?.asString?.let { traceIds.add(it) }
                if (needsLog) obj.get("logId")?.asString?.let { logIds.add(it) }
            }
        }

        val parentsMap = mutableMapOf<String, JsonObject>()

        if (traceIds.isNotEmpty()) {
            val traceQuery = JsonObject().apply {
                add("selector", JsonObject().apply {
                    add("_id", JsonObject().apply {
                        val inArray = JsonArray()
                        traceIds.forEach { inArray.add(it) }
                        add("\$in", inArray)
                    })
                })
                addProperty("limit", traceIds.size)
            }
            dbManager.findDocs(dbName, traceQuery).forEach { doc ->
                val obj = doc.asJsonObject
                parentsMap[obj.get("_id").asString] = obj
            }
        }

        if (logIds.isNotEmpty()) {
            val logQuery = JsonObject().apply {
                add("selector", JsonObject().apply {
                    add("_id", JsonObject().apply {
                        val inArray = JsonArray()
                        logIds.forEach { inArray.add(it) }
                        add("\$in", inArray)
                    })
                })
                addProperty("limit", logIds.size)
            }
            dbManager.findDocs(dbName, logQuery).forEach { doc ->
                val obj = doc.asJsonObject
                parentsMap[obj.get("_id").asString] = obj
            }
        }

        val sortedList = rawResult.map { it.asJsonObject }.sortedWith(Comparator { a, b ->
            var cmp = 0
            for (order in query.orderBy) {
                val segments = fieldMapper.resolve(order.scope, order.attribute).segments

                var valA = extractValueBySegments(a, segments)
                if (valA == null && order.scope != query.collection) {
                    val parentId = if (order.scope == PqlScope.TRACE) a.get("traceId")?.asString else a.get("logId")?.asString
                    val docA = parentId?.let { parentsMap[it] }
                    if (docA != null) valA = extractValueBySegments(docA, segments)
                }

                var valB = extractValueBySegments(b, segments)
                if (valB == null && order.scope != query.collection) {
                    val parentId = if (order.scope == PqlScope.TRACE) b.get("traceId")?.asString else b.get("logId")?.asString
                    val docB = parentId?.let { parentsMap[it] }
                    if (docB != null) valB = extractValueBySegments(docB, segments)
                }

                cmp = compareValues(valA, valB)
                if (order.direction == SortDirection.DESC) {
                    cmp = -cmp
                }
                if (cmp != 0) break
            }
            cmp
        })

        var stream = sortedList.asSequence()
        query.offset?.let { stream = stream.drop(it) }
        query.limit?.let { stream = stream.take(it) }

        val finalResult = JsonArray()
        stream.forEach { finalResult.add(it) }
        return finalResult
    }

    private fun getCrossScope(condition: PqlCondition, collectionScope: PqlScope): PqlScope? {
        return when (condition) {
            is PqlCondition.Simple -> if (condition.scope != collectionScope) condition.scope else null
            is PqlCondition.And -> condition.conditions.firstNotNullOfOrNull { getCrossScope(it, collectionScope) }
            is PqlCondition.Or -> condition.conditions.firstNotNullOfOrNull { getCrossScope(it, collectionScope) }
            is PqlCondition.Not -> getCrossScope(condition.condition, collectionScope)
            is PqlCondition.AlwaysFalse -> null
        }
    }

    private fun resolveCrossScopeConditions(conditions: List<PqlCondition>, collectionScope: PqlScope): List<String> {
        val outerScope = conditions.firstNotNullOfOrNull { getCrossScope(it, collectionScope) } ?: return emptyList()

        val outerQuery = PqlQuery(
            collection = outerScope,
            projections = emptyList(),
            conditions = conditions,
            selectAll = true
        )
        val outerCouchQuery = translator.translate(outerQuery)
        outerCouchQuery.addProperty("limit", 100000)
        val outerDocs = dbManager.findDocs(dbName, outerCouchQuery)

        val outerIds = outerDocs.mapNotNull { doc ->
            if (doc.isJsonObject) doc.asJsonObject.get("_id")?.asString else null
        }.distinct()

        if (outerIds.isEmpty()) return emptyList()

        if (outerScope == PqlScope.LOG && collectionScope == PqlScope.EVENT) {
            val traceQuery = JsonObject().apply {
                add("selector", JsonObject().apply {
                    addProperty("docType", "trace")
                    val inArray = JsonArray()
                    outerIds.forEach { inArray.add(it) }
                    add("logId", JsonObject().apply { add("\$in", inArray) })
                })
                addProperty("limit", 100000)
            }
            val traceDocs = dbManager.findDocs(dbName, traceQuery)
            return traceDocs.mapNotNull { doc ->
                if (doc.isJsonObject) doc.asJsonObject.get("_id")?.asString else null
            }.distinct()
        }

        return outerIds
    }

    /**
     * Wstrzykuje oryginalne właściwości śladu bezpośrednio do obiektu zdarzenia,
     * aby zapobiec nadpisywaniu (tzw. shadowing) przez atrybuty XES o tych samych kluczach.
     */
    private fun injectTraceAttributes(eventObj: JsonObject, traceDoc: JsonObject?) {
        if (traceDoc == null) return
        val traceName = traceDoc.get("originalTraceId")?.asString
        if (traceName != null) {
            eventObj.addProperty("t:name", traceName)
            eventObj.addProperty("t:concept:name", traceName)
            eventObj.addProperty("trace:name", traceName)
            eventObj.addProperty("trace:concept:name", traceName)
        }
        traceDoc.getAsJsonObject("xes_attributes")?.entrySet()?.forEach { (k, v) ->
            eventObj.add("t:$k", v)
            eventObj.add("trace:$k", v)
        }
    }

    /**
     * Główny hub kierujący do odpowiedniej strategii grupowania (warianty lub tradycyjne).
     */
    private fun formatGroupedResponse(docs: JsonArray, query: PqlQuery): JsonArray {
        val hoistedEventGroupField = query.groupBy.firstOrNull { it.attribute.startsWith("^") }

        return if (hoistedEventGroupField != null) {
            formatVariantGroupedResponse(docs, query, hoistedEventGroupField)
        } else {
            formatFlatGroupedResponse(docs, query)
        }
    }

    /**
     * Strategia grupowania płaskiego w stylu baz SQL.
     * Grupuje zdarzenia na podstawie określonych atrybutów, wyliczając na nich agregacje.
     */
    private fun formatFlatGroupedResponse(docs: JsonArray, query: PqlQuery): JsonArray {
        val traceIds = mutableSetOf<String>()
        docs.forEach {
            if (it.isJsonObject) it.asJsonObject.get("traceId")?.asString?.let { tId -> traceIds.add(tId) }
        }

        val traceDocsMap = mutableMapOf<String, JsonObject>()
        if (traceIds.isNotEmpty()) {
            val traceQuery = JsonObject().apply {
                add("selector", JsonObject().apply {
                    add("_id", JsonObject().apply {
                        val inArray = JsonArray()
                        traceIds.forEach { inArray.add(it) }
                        add("\$in", inArray)
                    })
                })
                addProperty("limit", traceIds.size)
            }
            dbManager.findDocs(dbName, traceQuery).forEach { doc ->
                val obj = doc.asJsonObject
                val tId = obj.get("_id")?.asString
                if (tId != null) traceDocsMap[tId] = obj
            }
        }

        val flatGroups = mutableMapOf<String, MutableList<JsonObject>>()
        for (element in docs) {
            if (!element.isJsonObject) continue
            val doc = element.asJsonObject
            val tId = doc.get("traceId")?.asString ?: "unknown"

            val groupKey = query.groupBy.joinToString("|") { gb ->
                val targetDoc = if (gb.scope == PqlScope.TRACE) traceDocsMap[tId] ?: doc else doc
                val segments = fieldMapper.resolve(gb.scope, gb.attribute).segments
                var extracted = extractValueBySegments(targetDoc, segments)
                if (extracted == null && gb.scope == PqlScope.TRACE && (gb.attribute == "name" || gb.attribute == "concept:name")) {
                    extracted = targetDoc.get("originalTraceId")?.asString
                }
                extracted?.toString() ?: "null"
            }
            flatGroups.getOrPut(groupKey) { mutableListOf() }.add(doc)
        }

        val finalDocs = mutableListOf<JsonObject>()
        flatGroups.forEach { (_, eventsInGroup) ->
            val groupResult = JsonObject()
            groupResult.addProperty("count", eventsInGroup.size)

            val representative = eventsInGroup.first()
            val tId = representative.get("traceId")?.asString ?: "unknown"

            val formattedRep = formatter.format(JsonArray().apply { add(representative) }, query)[0].asJsonObject
            injectTraceAttributes(formattedRep, traceDocsMap[tId])

            query.projections.forEach { proj ->
                if (proj.function != null || proj.arithmeticExpression != null) {
                    val value = evaluateGroupProjection(eventsInGroup, proj, traceDocsMap[tId])
                    val cleanKey = proj.raw.replace(" ", "")

                    if (value != null) {
                        formattedRep.addProperty(proj.raw, value.toString())
                        formattedRep.addProperty(cleanKey, value.toString())
                    }
                }
            }

            val representativeEvents = JsonArray()
            representativeEvents.add(formattedRep)
            groupResult.add("events", representativeEvents)
            finalDocs.add(groupResult)
        }

        val sortedResult = JsonArray()
        var stream = finalDocs.asSequence()
        query.limit?.let { stream = stream.take(it) }
        stream.forEach { sortedResult.add(it) }

        return sortedResult
    }

    /**
     * Strategia grupowania wariantów (Process Mining).
     * Wykorzystuje hoisting (^), aby tworzyć i grupować pełne sekwencje historyczne zdarzeń.
     */
    private fun formatVariantGroupedResponse(docs: JsonArray, query: PqlQuery, hoistedGroupField: pql.model.PqlGroupByField): JsonArray {
        val attrName = hoistedGroupField.attribute.removePrefix("^").removePrefix("e:")

        val eventsByTrace = mutableMapOf<String, MutableList<JsonObject>>()
        for (element in docs) {
            if (!element.isJsonObject) continue
            val doc = element.asJsonObject
            val traceId = doc.get("traceId")?.asString ?: "unknown"
            eventsByTrace.getOrPut(traceId) { mutableListOf() }.add(doc)
        }

        val innerSortRule = query.orderBy.firstOrNull { it.scope == PqlScope.EVENT && !it.attribute.lowercase().startsWith("count") }
        val outerSortRule = query.orderBy.firstOrNull { it.attribute.lowercase().startsWith("count") }
        val traceGroupKeys = query.groupBy.filter { !it.attribute.startsWith("^") && it.scope != PqlScope.EVENT }

        val validTraceIds = eventsByTrace.keys.filter { it != "unknown" }.toSet()
        val traceEventOrder = mutableMapOf<String, List<String>>()
        val traceDocsMap = mutableMapOf<String, JsonObject>()

        if (validTraceIds.isNotEmpty()) {
            val traceQuery = JsonObject().apply {
                add("selector", JsonObject().apply {
                    add("_id", JsonObject().apply {
                        val inArray = JsonArray()
                        validTraceIds.forEach { inArray.add(it) }
                        add("\$in", inArray)
                    })
                })
                addProperty("limit", validTraceIds.size)
            }

            dbManager.findDocs(dbName, traceQuery).forEach { doc ->
                val obj = doc.asJsonObject
                val tId = obj.get("_id")?.asString

                if (tId != null) {
                    traceDocsMap[tId] = obj
                    val eventIdsArr = obj.getAsJsonArray("eventIds")
                    if (eventIdsArr != null) {
                        val idList = mutableListOf<String>()
                        for (el in eventIdsArr) {
                            if (el.isJsonPrimitive) idList.add(el.asString)
                        }
                        traceEventOrder[tId] = idList
                    }
                }
            }
        }

        val traceGroups = mutableMapOf<String, MutableList<List<JsonObject>>>()

        for ((tId, events) in eventsByTrace) {
            val orderList = traceEventOrder[tId]

            events.sortWith(Comparator { a, b ->
                var cmp = 0
                if (innerSortRule != null) {
                    val segments = fieldMapper.resolve(innerSortRule.scope, innerSortRule.attribute).segments
                    val valA = extractValueBySegments(a, segments)
                    val valB = extractValueBySegments(b, segments)
                    cmp = compareValues(valA, valB)
                    if (innerSortRule.direction == SortDirection.DESC) cmp = -cmp
                } else {
                    if (orderList != null) {
                        val idA = a.get("_id")?.asString
                        val idB = b.get("_id")?.asString
                        val idxA = if (idA != null) orderList.indexOf(idA) else -1
                        val idxB = if (idB != null) orderList.indexOf(idB) else -1

                        if (idxA != -1 && idxB != -1) {
                            cmp = idxA.compareTo(idxB)
                        }
                    }

                    if (cmp == 0) {
                        val tsA = extractTimestamp(a)
                        val tsB = extractTimestamp(b)
                        cmp = tsA.compareTo(tsB)

                        if (cmp == 0) {
                            cmp = (a.get("_id")?.asString ?: "").compareTo(b.get("_id")?.asString ?: "")
                            if (cmp == 0) cmp = a.toString().compareTo(b.toString())
                        }
                    }
                }
                cmp
            })

            var prefixKey = ""
            if (traceGroupKeys.isNotEmpty() && events.isNotEmpty()) {
                val representative = events.first()
                prefixKey = traceGroupKeys.joinToString("|") { gb ->
                    val segments = fieldMapper.resolve(gb.scope, gb.attribute).segments
                    val targetDoc = when (gb.scope) {
                        PqlScope.TRACE -> traceDocsMap[tId] ?: representative
                        else -> representative
                    }
                    var extracted = extractValueBySegments(targetDoc, segments)
                    if (extracted == null && gb.scope == PqlScope.TRACE && (gb.attribute == "name" || gb.attribute == "concept:name")) {
                        extracted = targetDoc.get("originalTraceId")?.asString
                    }
                    extracted?.toString() ?: "null"
                } + "|"
            }

            val sequenceKey = prefixKey + events.joinToString(",") { evt ->
                evt.get("activity")?.asString ?: evt.getAsJsonObject("xes_attributes")?.get(attrName)?.asString ?: "null"
            }

            traceGroups.getOrPut(sequenceKey) { mutableListOf() }.add(events)
        }

        val finalDocs = mutableListOf<JsonObject>()
        traceGroups.forEach { (_, listOfTraces) ->
            val groupResult = JsonObject()
            groupResult.addProperty("count", listOfTraces.size)

            val representativeEvents = JsonArray()
            listOfTraces.first().forEach { eventDoc ->
                val formattedEvent = formatter.format(JsonArray().apply { add(eventDoc) }, query)[0].asJsonObject
                val tId = eventDoc.get("traceId")?.asString
                injectTraceAttributes(formattedEvent, traceDocsMap[tId])
                representativeEvents.add(formattedEvent)
            }
            groupResult.add("events", representativeEvents)
            finalDocs.add(groupResult)
        }

        if (outerSortRule != null) {
            finalDocs.sortWith(Comparator { a, b ->
                val valA = extractValueBySegments(a, listOf(outerSortRule.attribute)) ?: extractValueBySegments(a, listOf("count"))
                val valB = extractValueBySegments(b, listOf(outerSortRule.attribute)) ?: extractValueBySegments(b, listOf("count"))

                var cmp = compareValues(valA, valB)
                if (outerSortRule.direction == SortDirection.DESC) cmp = -cmp
                cmp
            })
        } else {
            finalDocs.sortByDescending { it.get("count").asInt }
        }

        var stream = finalDocs.asSequence()
        query.offset?.let { stream = stream.drop(it) }
        query.limit?.let { stream = stream.take(it) }

        val sortedResult = JsonArray()
        stream.forEach { sortedResult.add(it) }
        return sortedResult
    }

    private fun extractTimestamp(doc: JsonObject): String {
        return doc.get("timestamp")?.asString
            ?: doc.getAsJsonObject("xes_attributes")?.get("time:timestamp")?.asString
            ?: ""
    }

    /**
     * Rekursywnie wydobywa wartość z zagnieżdżonego obiektu JSON, używając listy segmentów.
     */
    private fun extractValueBySegments(doc: JsonObject, segments: List<String>): Any? {
        var current: com.google.gson.JsonElement? = doc
        for (segment in segments) {
            if (current == null || !current.isJsonObject) return null
            current = current.asJsonObject.get(segment)
        }
        if (current == null || current.isJsonNull) return null

        return when {
            current.isJsonPrimitive -> {
                val prim = current.asJsonPrimitive
                when {
                    prim.isNumber -> prim.asDouble
                    prim.isBoolean -> prim.asBoolean
                    else -> prim.asString
                }
            }
            else -> current.toString()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun compareValues(a: Any?, b: Any?): Int {
        if (a == null && b == null) return 0
        if (a == null) return -1
        if (b == null) return 1

        if (a is Double && b is Double) {
            return a.compareTo(b)
        }

        if (a is Boolean && b is Boolean) {
            return a.compareTo(b)
        }

        return a.toString().compareTo(b.toString())
    }

    // --- SILNIK OBLICZENIOWY DLA GRUP (AGREGACJE I MATEMATYKA) ---

    private fun evaluateGroupProjection(events: List<JsonObject>, proj: pql.model.PqlProjection, traceDoc: JsonObject?): Double? {
        if (proj.function is pql.model.PqlFunction.AggregationFunction) {
            return evaluateAggregation(events, proj.function, traceDoc)
        }
        if (proj.arithmeticExpression != null) {
            return evaluateMath(events, proj.arithmeticExpression, traceDoc)
        }
        return null
    }

    private fun evaluateAggregation(events: List<JsonObject>, func: pql.model.PqlFunction.AggregationFunction, traceDoc: JsonObject?): Double? {
        val aggType = func.component1() as pql.model.PqlFunction.Aggregation
        val scope = func.component2() as? pql.model.PqlScope ?: pql.model.PqlScope.EVENT
        val attribute = func.component3() as? String ?: return null

        val segments = fieldMapper.resolve(scope, attribute).segments
        val values = events.mapNotNull { evt ->
            val target = if (scope == pql.model.PqlScope.TRACE) traceDoc ?: evt else evt
            val extracted = extractValueBySegments(target, segments)

            if (extracted is String && (attribute.contains("time") || attribute.contains("timestamp"))) {
                try {
                    OffsetDateTime.parse(extracted).toInstant().toEpochMilli().toDouble()
                } catch(e: DateTimeParseException) {
                    extracted.toDoubleOrNull()
                }
            } else if (extracted is Number) {
                extracted.toDouble()
            } else {
                extracted?.toString()?.toDoubleOrNull()
            }
        }

        if (values.isEmpty()) return null

        return when (aggType.name) {
            "MIN" -> values.minOrNull()
            "MAX" -> values.maxOrNull()
            "SUM" -> values.sum()
            "AVG" -> values.average()
            "COUNT" -> values.size.toDouble()
            else -> null
        }
    }

    private fun evaluateMath(events: List<JsonObject>, expr: pql.model.PqlArithmeticExpression, traceDoc: JsonObject?): Double? {
        return when (expr) {
            is pql.model.PqlArithmeticExpression.Literal -> expr.value.toString().toDoubleOrNull()
            is pql.model.PqlArithmeticExpression.Attribute -> null
            is pql.model.PqlArithmeticExpression.FunctionCall -> {
                if (expr.function is pql.model.PqlFunction.AggregationFunction) {
                    evaluateAggregation(events, expr.function, traceDoc)
                } else null
            }
            is pql.model.PqlArithmeticExpression.Binary -> {
                val left = evaluateMath(events, expr.left, traceDoc) ?: return null
                val right = evaluateMath(events, expr.right, traceDoc) ?: return null
                when (expr.operator) {
                    pql.model.ArithmeticOperator.ADD -> left + right
                    pql.model.ArithmeticOperator.SUB -> left - right
                    pql.model.ArithmeticOperator.MUL -> left * right
                    pql.model.ArithmeticOperator.DIV -> if (right != 0.0) left / right else null
                }
            }
        }
    }
}
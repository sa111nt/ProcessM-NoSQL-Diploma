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
     * @param query Sparsowany obiekt zapytania PQL.
     * @return JsonArray zawierający sformatowane wyniki spełniające warunki zapytania.
     */
    fun execute(query: PqlQuery): JsonArray {
        if (query.conditions.any { it is PqlCondition.AlwaysFalse }) {
            return JsonArray()
        }

        // --- OPTYMALIZACJA: Spłaszczanie warunków AND ---
        // Parser często grupuje warunki w jedno drzewo PqlCondition.And.
        // Musimy je rozbić na płaską listę, by poprawnie oddzielić filtry relacyjne (l:id)
        // od docelowych filtrów na zdarzeniach (np. e:org:resource).
        val flatConditions = mutableListOf<PqlCondition>()
        fun flatten(c: PqlCondition) {
            if (c is PqlCondition.And) {
                c.conditions.forEach { flatten(it) }
            } else {
                flatConditions.add(c)
            }
        }
        query.conditions.forEach { flatten(it) }

        // --- 1. MOST RELACYJNY (Cross-Scope) ---
        val crossScopeConditions = flatConditions.filter { getCrossScope(it, query.collection) != null }
        val sameScopeConditions = flatConditions.filter { getCrossScope(it, query.collection) == null }
        var resolvedQuery = query.copy(conditions = sameScopeConditions)

        if (crossScopeConditions.isNotEmpty()) {
            val outerScope = crossScopeConditions.firstNotNullOfOrNull { getCrossScope(it, query.collection) }!!
            // Przekazujemy TYLKO warunki cross-scope, aby nie szukać właściwości eventów w logach
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
        val hasAggregations = resolvedQuery.projections.any { hasAggregation(it) } ||
                resolvedQuery.orderBy.any { hasAggregation(it) }

        println("DEBUG EXECUTOR: hasAggregations=$hasAggregations, orderBySize=${resolvedQuery.orderBy.size}")

        // Usuwamy limity dla CouchDB, jeśli operacje wymagają pamięciowej obróbki
        if (hasCrossScopeOrderBy || resolvedQuery.groupBy.isNotEmpty() || hasAggregations) {
            mangoQuery.remove("limit")
            mangoQuery.remove("skip")
            mangoQuery.remove("sort")
            mangoQuery.addProperty("limit", 100000)

            // --- OPTYMALIZACJA PAMIĘCI (PROJECTION PUSHDOWN) ---
            val requiredFields = mutableSetOf<String>()

            // Zawsze potrzebujemy identyfikatorów i struktury do łączenia relacji
            requiredFields.add("_id")
            requiredFields.add("docType")
            requiredFields.add("traceId")
            requiredFields.add("logId")
            requiredFields.add(fieldMapper.resolve(PqlScope.EVENT, "concept:name").toDotPath())

            val extractAttribute: (pql.model.PqlScope, String?) -> Unit = { scope, attr ->
                if (attr != null) {
                    val cleanAttr = attr.removePrefix("^^").removePrefix("^")
                    val dotPath = fieldMapper.resolve(scope, cleanAttr).toDotPath()
                    requiredFields.add(dotPath)
                }
            }

            fun extractFromExpr(expr: pql.model.PqlArithmeticExpression?) {
                if (expr == null) return
                when (expr) {
                    is pql.model.PqlArithmeticExpression.Attribute -> extractAttribute(expr.scope, expr.attribute)
                    is pql.model.PqlArithmeticExpression.FunctionCall -> {
                        if (expr.function is pql.model.PqlFunction.AggregationFunction) {
                            val scope = expr.function.scope ?: PqlScope.EVENT
                            extractAttribute(scope, expr.function.attribute)
                        }
                    }
                    is pql.model.PqlArithmeticExpression.Binary -> {
                        extractFromExpr(expr.left)
                        extractFromExpr(expr.right)
                    }
                    else -> {}
                }
            }

            resolvedQuery.groupBy.forEach { extractAttribute(it.scope, it.attribute) }

            resolvedQuery.projections.forEach { proj ->
                proj.scope?.let { extractAttribute(it, proj.attribute) }
                if (proj.function is pql.model.PqlFunction.AggregationFunction) {
                    val scope = proj.function.scope ?: PqlScope.EVENT
                    extractAttribute(scope, proj.function.attribute)
                }
                extractFromExpr(proj.arithmeticExpression)
            }

            resolvedQuery.orderBy.forEach { order ->
                extractAttribute(order.scope, order.attribute)
                if (order.function is pql.model.PqlFunction.AggregationFunction) {
                    val scope = order.function.scope ?: PqlScope.EVENT
                    extractAttribute(scope, order.function.attribute)
                }
                extractFromExpr(order.arithmeticExpression)
            }

            // Skanuj pola z klauzuli WHERE (żeby CouchDB mógł je odfiltrować!)
            resolvedQuery.conditions.forEach { cond ->
                if (cond is PqlCondition.Simple) {
                    extractAttribute(cond.scope, cond.attribute)
                }
            }

            val fieldsArray = JsonArray()
            requiredFields.forEach { fieldsArray.add(it) }
            mangoQuery.add("fields", fieldsArray)

            println("DEBUG: Uruchomiono optymalizację pamięci. Pobierane pola: $requiredFields")
        }

        if (resolvedQuery.orderBy.isNotEmpty() && !hasCrossScopeOrderBy && resolvedQuery.groupBy.isEmpty()) {
            val indexFields = mutableListOf("docType")
            for (order in resolvedQuery.orderBy) {
                indexFields.add(fieldMapper.resolve(order.scope, order.attribute).toDotPath())
            }
            dbManager.ensureIndex(dbName, indexFields)
        }

        var rawResult: JsonArray = dbManager.findDocs(dbName, mangoQuery)

        if (hasCrossScopeOrderBy && rawResult.size() > 0 && resolvedQuery.groupBy.isEmpty()) {
            rawResult = executeCrossScopeOrderBy(rawResult, resolvedQuery)
        }

        if (resolvedQuery.groupBy.isNotEmpty() || hasAggregations) {
            return formatGroupedResponse(rawResult, resolvedQuery)
        }

        return formatter.format(rawResult, resolvedQuery)
    }

    private fun hasAggregation(projection: pql.model.PqlProjection): Boolean {
        return projection.function is pql.model.PqlFunction.AggregationFunction ||
                (projection.arithmeticExpression != null && hasAggregationInExpr(projection.arithmeticExpression))
    }

    private fun hasAggregation(order: pql.model.PqlOrder): Boolean {
        return order.function is pql.model.PqlFunction.AggregationFunction ||
                (order.arithmeticExpression != null && hasAggregationInExpr(order.arithmeticExpression))
    }

    private fun hasAggregationInExpr(expr: pql.model.PqlArithmeticExpression): Boolean {
        return when (expr) {
            is pql.model.PqlArithmeticExpression.FunctionCall -> expr.function is pql.model.PqlFunction.AggregationFunction
            is pql.model.PqlArithmeticExpression.Binary -> hasAggregationInExpr(expr.left) || hasAggregationInExpr(expr.right)
            else -> false
        }
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

        // PQL scope hierarchy: LOG (0) > TRACE (1) > EVENT (2)
        val scopeOrderedBy = query.orderBy.sortedBy { it.scope.ordinal }

        val sortedList = rawResult.map { it.asJsonObject }.sortedWith(Comparator { a, b ->
            var cmp = 0
            for (order in scopeOrderedBy) {
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
        // Tylko ^ (sequence variant), ale nie ^^ (global hoisting)
        val hoistedEventGroupField = query.groupBy.firstOrNull { it.attribute.startsWith("^") && !it.attribute.startsWith("^^") }

        return if (hoistedEventGroupField != null) {
            formatVariantGroupedResponse(docs, query, hoistedEventGroupField)
        } else {
            formatFlatGroupedResponse(docs, query)
        }
    }

    /**
     * Sprawdza, czy w projekcjach lub klauzulach order by występuje funkcja agregująca z parametrem używającym ^^
     */
    private fun hasGlobalHoistingAggregation(query: PqlQuery): Boolean {
        val checkFunc = { func: pql.model.PqlFunction? ->
            func is pql.model.PqlFunction.AggregationFunction && func.attribute?.startsWith("^^") == true
        }

        val checkExpr: (pql.model.PqlArithmeticExpression?) -> Boolean = { expr ->
            var found = false
            fun traverse(e: pql.model.PqlArithmeticExpression?) {
                if (e == null) return
                when (e) {
                    is pql.model.PqlArithmeticExpression.FunctionCall -> if (checkFunc(e.function)) found = true
                    is pql.model.PqlArithmeticExpression.Binary -> { traverse(e.left); traverse(e.right) }
                    else -> {}
                }
            }
            traverse(expr)
            found
        }

        val inProj = query.projections.any { checkFunc(it.function) || checkExpr(it.arithmeticExpression) }
        val inOrder = query.orderBy.any { checkFunc(it.function) || checkExpr(it.arithmeticExpression) }

        return inProj || inOrder
    }

    /**
     * Strategia grupowania płaskiego w stylu baz SQL.
     * Grupuje zdarzenia na podstawie określonych atrybutów, wyliczając na nich agregacje.
     */
    private fun formatFlatGroupedResponse(docs: JsonArray, query: PqlQuery): JsonArray {
        val traceIds = mutableSetOf<String>()
        val logIds = mutableSetOf<String>()
        docs.forEach {
            if (it.isJsonObject) {
                val obj = it.asJsonObject
                obj.get("traceId")?.asString?.let { tId -> traceIds.add(tId) }
                obj.get("logId")?.asString?.let { lId -> logIds.add(lId) }
            }
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

        val logDocsMap = mutableMapOf<String, JsonObject>()
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
                val lId = obj.get("_id")?.asString
                if (lId != null) logDocsMap[lId] = obj
            }
        }

        val flatGroups = mutableMapOf<String, MutableList<JsonObject>>()

        val hasImplicitGlobalHoisting = query.groupBy.isEmpty() && hasGlobalHoistingAggregation(query)
        val needsPerTraceGrouping = !hasImplicitGlobalHoisting && (query.groupBy.isEmpty() || query.groupBy.all { it.scope == PqlScope.EVENT && !it.attribute.startsWith("^^") })

        for (element in docs) {
            if (!element.isJsonObject) continue
            val doc = element.asJsonObject
            val tId = doc.get("traceId")?.asString ?: "unknown"

            val tracePrefix = if (needsPerTraceGrouping) "$tId|" else ""

            val groupKey = if (query.groupBy.isEmpty()) {
                if (hasImplicitGlobalHoisting) "GLOBAL_GROUP" else tracePrefix
            } else {
                tracePrefix + query.groupBy.joinToString("|") { gb ->
                    val lId = doc.get("logId")?.asString
                    val targetDoc = when (gb.scope) {
                        PqlScope.LOG -> if (lId != null) (logDocsMap[lId] ?: doc) else doc
                        PqlScope.TRACE -> traceDocsMap[tId] ?: doc
                        else -> doc
                    }

                    val cleanAttr = gb.attribute.removePrefix("^^").removePrefix("^")
                    val segments = fieldMapper.resolve(gb.scope, cleanAttr).segments
                    var extracted = extractValueBySegments(targetDoc, segments)
                    if (extracted == null && gb.scope == PqlScope.TRACE && (gb.attribute == "name" || gb.attribute == "concept:name")) {
                        extracted = targetDoc.get("originalTraceId")?.asString
                    }
                    extracted?.toString() ?: "null"
                }
            }
            flatGroups.getOrPut(groupKey) { mutableListOf() }.add(doc)
        }

        val finalDocs = mutableListOf<JsonObject>()
        flatGroups.forEach { (_, eventsInGroup) ->
            val groupResult = JsonObject()
            groupResult.addProperty("count", eventsInGroup.size)

            val representative = eventsInGroup.first()
            val tId = representative.get("traceId")?.asString ?: "unknown"
            val lId = representative.get("logId")?.asString ?: "unknown"

            val formattedRep = formatter.format(JsonArray().apply { add(representative) }, query)[0].asJsonObject

            if (!hasImplicitGlobalHoisting) {
                injectTraceAttributes(formattedRep, traceDocsMap[tId])
            }

            query.projections.forEach { proj ->
                if (proj.function != null || proj.arithmeticExpression != null) {
                    val value = evaluateGroupProjection(eventsInGroup, proj, traceDocsMap[tId], logDocsMap[lId])
                    val cleanKey = proj.raw.replace(" ", "")

                    if (value != null) {
                        formattedRep.addProperty(proj.raw, value.toString())
                        formattedRep.addProperty(cleanKey, value.toString())
                    }
                }
            }

            query.orderBy.forEach { order ->
                if (order.function != null || order.arithmeticExpression != null) {
                    val dummyProj = pql.model.PqlProjection(scope = order.scope, attribute = order.attribute, raw = order.attribute,
                        function = order.function, arithmeticExpression = order.arithmeticExpression)
                    val value = evaluateGroupProjection(eventsInGroup, dummyProj, traceDocsMap[tId], logDocsMap[lId])
                    val cleanKey = dummyProj.raw.replace(" ", "")

                    if (value != null) {
                        formattedRep.addProperty(dummyProj.raw, value.toString())
                        formattedRep.addProperty(cleanKey, value.toString())
                    }
                }
            }

            val representativeEvents = JsonArray()
            representativeEvents.add(formattedRep)
            groupResult.add("events", representativeEvents)
            finalDocs.add(groupResult)
        }

        // --- BRAKUJĄCE SORTOWANIE ZGRUPOWANYCH WYNIKÓW ---
        if (query.orderBy.isNotEmpty()) {
            finalDocs.sortWith(Comparator { a, b ->
                var cmp = 0
                val repA = a.getAsJsonArray("events")[0].asJsonObject
                val repB = b.getAsJsonArray("events")[0].asJsonObject

                for (order in query.orderBy) {
                    val keyRaw = order.attribute
                    val keyClean = keyRaw.replace(" ", "")

                    var valA: Any? = repA.get(keyRaw)?.let { if (it.isJsonPrimitive && it.asJsonPrimitive.isNumber) it.asDouble else it.asString }
                    if (valA == null) valA = repA.get(keyClean)?.let { if (it.isJsonPrimitive && it.asJsonPrimitive.isNumber) it.asDouble else it.asString }

                    var valB: Any? = repB.get(keyRaw)?.let { if (it.isJsonPrimitive && it.asJsonPrimitive.isNumber) it.asDouble else it.asString }
                    if (valB == null) valB = repB.get(keyClean)?.let { if (it.isJsonPrimitive && it.asJsonPrimitive.isNumber) it.asDouble else it.asString }

                    if (valA == null) valA = extractValueBySegments(repA, fieldMapper.resolve(order.scope, order.attribute).segments)
                    if (valB == null) valB = extractValueBySegments(repB, fieldMapper.resolve(order.scope, order.attribute).segments)

                    // Konwertujemy tekstową notację naukową na liczby do bezpiecznego porównania matematycznego
                    val numA = valA?.toString()?.toDoubleOrNull()
                    val numB = valB?.toString()?.toDoubleOrNull()

                    val finalValA = numA ?: valA
                    val finalValB = numB ?: valB

                    cmp = compareValues(finalValA, finalValB)

                    if (order.direction == SortDirection.DESC) {
                        cmp = -cmp
                    }
                    if (cmp != 0) break
                }
                cmp
            })
        }
        // --- KONIEC SORTOWANIA ---

        val sortedResult = JsonArray()
        var stream = finalDocs.asSequence()
        query.offset?.let { stream = stream.drop(it) }
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

        val innerSortRule = query.orderBy.firstOrNull { it.scope == PqlScope.EVENT && !it.attribute.lowercase().startsWith("count") && !it.attribute.matches(Regex(".*(?:min|max|avg|sum|count)\\(.*\\).*")) }
        val outerSortRule = query.orderBy.firstOrNull { order ->
            val attr = order.attribute.lowercase()
            attr.startsWith("count") || attr.startsWith("min") || attr.startsWith("max") || attr.startsWith("avg") || attr.startsWith("sum")
        }
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

        val logDocsMap = mutableMapOf<String, JsonObject>()
        val logIds = validTraceIds.mapNotNull { eventsByTrace[it]?.firstOrNull()?.get("logId")?.asString }.toSet()
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
                val lId = obj.get("_id")?.asString
                if (lId != null) logDocsMap[lId] = obj
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
                    val lId = representative.get("logId")?.asString
                    val targetDoc = when (gb.scope) {
                        PqlScope.LOG -> if (lId != null) (logDocsMap[lId] ?: representative) else representative
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

            if (outerSortRule != null && outerSortRule.attribute.contains("(")) {
                val orderAttr = outerSortRule.attribute
                val match = Regex("""([a-zA-Z]+)\((.*)\)""").find(orderAttr)
                if (match != null) {
                    val funcName = match.groupValues[1].lowercase()
                    val innerAttrRaw = match.groupValues[2]

                    val cleanInnerAttr = innerAttrRaw.replace("^", "")
                    val scopePrefix = if (cleanInnerAttr.contains(":")) cleanInnerAttr.substringBefore(":") else "e"
                    val finalAttr = if (cleanInnerAttr.contains(":")) cleanInnerAttr.substringAfter(":") else cleanInnerAttr

                    val scope = when (scopePrefix.lowercase()) {
                        "l", "log" -> PqlScope.LOG
                        "t", "trace" -> PqlScope.TRACE
                        else -> PqlScope.EVENT
                    }

                    val aggType = when (funcName) {
                        "min" -> pql.model.PqlFunction.Aggregation.MIN
                        "max" -> pql.model.PqlFunction.Aggregation.MAX
                        "sum" -> pql.model.PqlFunction.Aggregation.SUM
                        "avg" -> pql.model.PqlFunction.Aggregation.AVG
                        else -> pql.model.PqlFunction.Aggregation.COUNT
                    }

                    val aggFunc = pql.model.PqlFunction.AggregationFunction(aggType, scope, finalAttr)
                    val allEvents = listOfTraces.flatten()
                    val repEvent = allEvents.firstOrNull()
                    val lId = repEvent?.get("logId")?.asString
                    val aggValue = evaluateAggregation(allEvents, aggFunc, null, if (lId != null) logDocsMap[lId] else null)

                    if (aggValue != null) {
                        when (aggValue) {
                            is Number -> groupResult.addProperty(orderAttr, aggValue)
                            is Boolean -> groupResult.addProperty(orderAttr, aggValue)
                            else -> groupResult.addProperty(orderAttr, aggValue.toString())
                        }
                    }
                }
            }

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

    private fun evaluateGroupProjection(events: List<JsonObject>, proj: pql.model.PqlProjection, traceDoc: JsonObject?, logDoc: JsonObject? = null): Any? {
        if (proj.function is pql.model.PqlFunction.AggregationFunction) {
            return evaluateAggregation(events, proj.function, traceDoc, logDoc)
        }
        if (proj.arithmeticExpression != null) {
            return evaluateMath(events, proj.arithmeticExpression, traceDoc, logDoc)
        }
        return null
    }

    private fun evaluateAggregation(events: List<JsonObject>, func: pql.model.PqlFunction.AggregationFunction, traceDoc: JsonObject?, logDoc: JsonObject? = null): Any? {
        val aggType = func.type
        val scope = func.scope ?: pql.model.PqlScope.EVENT
        val attribute = func.attribute ?: return null

        val cleanAttribute = attribute.removePrefix("^^").removePrefix("^")

        val segments = fieldMapper.resolve(scope, cleanAttribute).segments
        val rawValues = events.mapNotNull { evt ->
            val target = when (scope) {
                pql.model.PqlScope.LOG -> logDoc ?: evt
                pql.model.PqlScope.TRACE -> traceDoc ?: evt
                else -> evt
            }
            val ext = extractValueBySegments(target, segments)
            ext
        }

        if (rawValues.isEmpty()) return null

        // KRYTYCZNA POPRAWKA DLA FUNKCJI COUNT:
        // Zliczamy elementy od razu, zanim algorytm odrzuci wartości tekstowe przy próbie
        // rzutowania na Double (co powodowało puste wyniki dla count(tekst)).
        if (aggType.name == "COUNT") {
            return rawValues.size.toDouble()
        }

        val isStringAgg = rawValues.any { it is String && it.toDoubleOrNull() == null } && (cleanAttribute.contains("name") || !cleanAttribute.contains("time"))

        if (isStringAgg && (aggType.name == "MIN" || aggType.name == "MAX")) {
            val strValues = rawValues.map { it.toString() }
            return if (aggType.name == "MIN") strValues.minOrNull() else strValues.maxOrNull()
        }

        val values = rawValues.mapNotNull { extracted ->
            if (extracted is String && (cleanAttribute.contains("time") || cleanAttribute.contains("timestamp"))) {
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
            else -> null
        }
    }

    private fun evaluateMath(events: List<JsonObject>, expr: pql.model.PqlArithmeticExpression, traceDoc: JsonObject?, logDoc: JsonObject? = null): Any? {
        return when (expr) {
            is pql.model.PqlArithmeticExpression.Literal -> expr.value.toString().toDoubleOrNull()
            is pql.model.PqlArithmeticExpression.Attribute -> null
            is pql.model.PqlArithmeticExpression.FunctionCall -> {
                if (expr.function is pql.model.PqlFunction.AggregationFunction) {
                    evaluateAggregation(events, expr.function, traceDoc, logDoc)
                } else null
            }
            is pql.model.PqlArithmeticExpression.Binary -> {
                val leftRaw = evaluateMath(events, expr.left, traceDoc, logDoc) ?: return null
                val rightRaw = evaluateMath(events, expr.right, traceDoc, logDoc) ?: return null
                val leftVal = leftRaw as? Double ?: leftRaw.toString().toDoubleOrNull() ?: return null
                val rightVal = rightRaw as? Double ?: rightRaw.toString().toDoubleOrNull() ?: return null

                when (expr.operator) {
                    pql.model.ArithmeticOperator.ADD -> leftVal + rightVal
                    pql.model.ArithmeticOperator.SUB -> leftVal - rightVal
                    pql.model.ArithmeticOperator.MUL -> leftVal * rightVal
                    pql.model.ArithmeticOperator.DIV -> if (rightVal != 0.0) leftVal / rightVal else null
                }
            }
        }
    }
}
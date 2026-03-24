package pql.executor

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import db.CouchDBManager
import pql.model.PqlCondition
import pql.model.PqlDeleteQuery
import pql.model.PqlQuery
import pql.translator.PqlFieldMapper
import pql.translator.PqlToCouchDbTranslator
import pql.model.PqlScope
import pql.model.SortDirection
import java.time.Instant
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

private fun JsonObject.addComputedProperty(rawKey: String, value: Any?) {
    if (value == null) return
    when (value) {
        is Long, is Int, is Short, is Byte -> this.addProperty(rawKey, value as Number)
        is Double -> if (value % 1 == 0.0) this.addProperty(rawKey, value.toLong()) else this.addProperty(rawKey, value)
        is Float -> if (value % 1 == 0.0f) this.addProperty(rawKey, value.toLong()) else this.addProperty(rawKey, value)
        is Number -> this.addProperty(rawKey, value)
        is Boolean -> this.addProperty(rawKey, value)
        else -> this.addProperty(rawKey, value.toString())
    }
}

class PqlQueryExecutor(
    private val dbManager: CouchDBManager,
    private val dbName: String
) {
    private val translator = PqlToCouchDbTranslator()
    private val formatter = PqlResultFormatter(PqlFieldMapper())

    companion object {
        private val fieldMapper = PqlFieldMapper()
        private val dateParsingCache = java.util.concurrent.ConcurrentHashMap<String, Double>()
        private val tPrefixCache = java.util.concurrent.ConcurrentHashMap<String, String>()
        private val lPrefixCache = java.util.concurrent.ConcurrentHashMap<String, String>()

        private fun getTPrefix(k: String): String = tPrefixCache.getOrPut(k) { "t:$k" }
        private fun getLPrefix(k: String): String = lPrefixCache.getOrPut(k) { "l:$k" }

        private fun resolveNested(baseObj: JsonObject?, attr: String): JsonElement? {
            if (baseObj == null) return null
            if (baseObj.has(attr)) return baseObj.get(attr)

            val parts = attr.split(":")
            var curr: JsonElement? = baseObj
            for (p in parts) {
                if (curr == null || !curr.isJsonObject) return null
                val actualKey = curr.asJsonObject.keySet().firstOrNull { it.equals(p, ignoreCase = true) }
                curr = if (actualKey != null) curr.asJsonObject.get(actualKey) else null
            }
            return curr
        }

        fun extractRobustValue(doc: JsonObject, scope: PqlScope, attribute: String): Any? {
            val resolvedAttribute = fieldMapper.getStandardName(attribute)
            val cleanAttrOriginal = resolvedAttribute.removePrefix("^^").removePrefix("^").removePrefix("[").removeSuffix("]")
            val cleanAttrLower = cleanAttrOriginal.lowercase()

            val mappedAttr = fieldMapper.getStandardName(cleanAttrLower)

            val docType = doc.get("docType")?.takeIf { !it.isJsonNull }?.asString?.intern() ?: "event"
            val docLevel = when(docType) { "log" -> 1; "trace" -> 2; else -> 3 }
            val targetLevel = when(scope) { PqlScope.LOG -> 1; PqlScope.TRACE -> 2; else -> 3 }

            if (docLevel > targetLevel) {
                val injectedKey = when(scope) { PqlScope.TRACE -> "t:"; PqlScope.LOG -> "l:"; else -> "" } + cleanAttrOriginal
                if (doc.has(injectedKey)) {
                    val v = doc.get(injectedKey)
                    return if (v.isJsonPrimitive && v.asJsonPrimitive.isNumber) v.asDouble else if (!v.isJsonNull) v.asString?.intern() else null
                }
                if (scope == PqlScope.TRACE) {
                    if (cleanAttrLower == "name" || cleanAttrLower == "concept:name") return doc.get("t:concept:name")?.takeIf { !it.isJsonNull }?.asString?.intern() ?: doc.get("t:name")?.takeIf { !it.isJsonNull }?.asString?.intern() ?: doc.get("traceId")?.takeIf { !it.isJsonNull }?.asString?.substringAfterLast("_")?.intern()
                } else if (scope == PqlScope.LOG) {
                    if (cleanAttrLower == "name" || cleanAttrLower == "concept:name") return doc.get("l:concept:name")?.takeIf { !it.isJsonNull }?.asString?.intern() ?: doc.get("l:name")?.takeIf { !it.isJsonNull }?.asString?.intern() ?: doc.get("logId")?.takeIf { !it.isJsonNull }?.asString?.intern()
                }
                return null
            }

            val classifierKey = if (cleanAttrOriginal.startsWith("classifier:")) "c:" + cleanAttrOriginal.removePrefix("classifier:")
            else if (cleanAttrOriginal.startsWith("e:c:")) "c:" + cleanAttrOriginal.removePrefix("e:c:")
            else if (cleanAttrOriginal.startsWith("c:")) cleanAttrOriginal
            else null

            if (classifierKey != null && doc.has(classifierKey)) {
                return doc.get(classifierKey).asString.intern()
            }

            val baseAttr = cleanAttrOriginal.removePrefix("e:c:").removePrefix("c:").removePrefix("classifier:")
            val isClassifier = cleanAttrOriginal.startsWith("c:") || cleanAttrOriginal.startsWith("classifier:") || cleanAttrOriginal.startsWith("e:c:") || cleanAttrOriginal.startsWith("t:c:") || cleanAttrOriginal.startsWith("l:c:")

            if (isClassifier) {
                val mappedFields = fieldMapper.getImplicitClassifierMapping(baseAttr)
                val vals = mappedFields.map { part ->
                    val resolved = extractRobustValue(doc, scope, part)?.toString() ?: ""
                    if (resolved == "null" || resolved.isEmpty()) "" else resolved
                }.filter { it.isNotEmpty() }
                if (vals.isNotEmpty()) return vals.joinToString("+").intern()
            }

            val segments = fieldMapper.resolve(scope, cleanAttrOriginal).segments
            var current: com.google.gson.JsonElement? = doc
            for (segment in segments) {
                if (current == null || !current.isJsonObject) { current = null; break }
                val obj = current.asJsonObject
                val actualKey = obj.keySet().firstOrNull { it.equals(segment, ignoreCase = true) }
                if (actualKey != null) {
                    current = obj.get(actualKey)
                } else {
                    current = null
                    break
                }
            }

            if (current != null && !current.isJsonNull) {
                return if (current.isJsonPrimitive) {
                    val prim = current.asJsonPrimitive
                    when {
                        prim.isNumber -> prim.asDouble
                        prim.isBoolean -> prim.asBoolean
                        else -> prim.asString.intern()
                    }
                } else current.toString().intern()
            }

            val attrContainer = when (docType) {
                "log" -> doc.getAsJsonObject("log_attributes") ?: doc
                "trace", "event" -> doc.getAsJsonObject("xes_attributes") ?: doc
                else -> doc
            }

            if (attrContainer.has(mappedAttr)) {
                val exactVal = attrContainer.get(mappedAttr)
                if (!exactVal.isJsonNull) {
                    return if (exactVal.isJsonPrimitive) {
                        val prim = exactVal.asJsonPrimitive
                        if (prim.isNumber) prim.asDouble else if (prim.isBoolean) prim.asBoolean else prim.asString.intern()
                    } else exactVal.toString().intern()
                }
            }

            val manualNested = resolveNested(attrContainer, mappedAttr)
            if (manualNested != null && !manualNested.isJsonNull) {
                return if (manualNested.isJsonPrimitive) {
                    if (manualNested.asJsonPrimitive.isNumber) manualNested.asDouble else if (manualNested.asJsonPrimitive.isBoolean) manualNested.asBoolean else manualNested.asString.intern()
                } else manualNested.toString().intern()
            }

            if (scope == PqlScope.EVENT) {
                if (cleanAttrLower == "name" || cleanAttrLower == "concept:name" || cleanAttrLower == "activity") return doc.get("activity")?.takeIf { !it.isJsonNull }?.asString?.intern()
                if (cleanAttrLower.contains("timestamp")) {
                    val ts = doc.get("timestamp")?.takeIf { !it.isJsonNull }?.asString?.intern() ?: doc.getAsJsonObject("xes_attributes")?.get("time:timestamp")?.takeIf { !it.isJsonNull }?.asString?.intern() ?: ""
                    if (ts.isNotEmpty()) return ts
                }
            }
            return null
        }
    }

    private fun getCrossScope(condition: PqlCondition, collectionScope: PqlScope): PqlScope? {
        if (condition is PqlCondition.Simple) {
            if (condition.scope != collectionScope) return condition.scope
            return null
        }
        if (condition is PqlCondition.And) return condition.conditions.firstNotNullOfOrNull { getCrossScope(it, collectionScope) }
        if (condition is PqlCondition.Or) return condition.conditions.firstNotNullOfOrNull { getCrossScope(it, collectionScope) }
        if (condition is PqlCondition.Not) return getCrossScope(condition.condition, collectionScope)
        return null
    }

    private fun resolveCrossScopeConditions(conditions: List<PqlCondition>, collectionScope: PqlScope): List<String> {
        val outerScope = conditions.firstNotNullOfOrNull { getCrossScope(it, collectionScope) } ?: return emptyList()

        val outerQuery = PqlQuery(
            collection = outerScope,
            projections = emptyList(),
            conditions = emptyList(),
            selectAll = true
        )
        val outerCouchQuery = translator.translate(outerQuery)
        outerCouchQuery.addProperty("limit", 100000)

        val parentIds = mutableListOf<String>()
        dbManager.findDocsStream(dbName, outerCouchQuery) { doc ->
            if (conditions.all { evaluateConditionInMemory(doc, it, outerScope) }) {
                doc.get("_id")?.takeIf { !it.isJsonNull }?.asString?.let { parentIds.add(it) }
            }
        }

        return parentIds.distinct()
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

    private fun extractScopeFromArithmetic(expr: pql.model.PqlArithmeticExpression): PqlScope? {
        return when (expr) {
            is pql.model.PqlArithmeticExpression.Attribute -> expr.scope
            is pql.model.PqlArithmeticExpression.Binary -> {
                extractScopeFromArithmetic(expr.left) ?: extractScopeFromArithmetic(expr.right)
            }
            is pql.model.PqlArithmeticExpression.FunctionCall -> {
                when (val func = expr.function) {
                    is pql.model.PqlFunction.AggregationFunction -> func.scope
                    is pql.model.PqlFunction.ScalarFunction1 -> extractScopeFromArithmetic(func.expression)
                    is pql.model.PqlFunction.ScalarFunction0 -> null
                }
            }
            is pql.model.PqlArithmeticExpression.Literal -> null
        }
    }

    private fun getScopeLevel(scope: PqlScope): Int = when(scope) { PqlScope.LOG -> 1; PqlScope.TRACE -> 2; PqlScope.EVENT -> 3 }

    private fun evaluateConditionInMemory(doc: JsonObject, cond: PqlCondition, targetScope: PqlScope): Boolean {
        when (cond) {
            is PqlCondition.Simple -> {
                val extracted = extractRobustValue(doc, cond.scope, cond.attribute)

                if (cond.operator == pql.model.PqlOperator.IS_NULL) return extracted == null || extracted.toString() == "null" || extracted.toString().isEmpty()
                if (cond.operator == pql.model.PqlOperator.IS_NOT_NULL) return extracted != null && extracted.toString() != "null" && extracted.toString().isNotEmpty()

                if (extracted == null) return false
                val expectedStr = cond.value?.toString()?.removeSurrounding("'")?.removeSurrounding("\"") ?: return false
                val extractedStr = extracted.toString()

                return when (cond.operator) {
                    pql.model.PqlOperator.EQ -> extractedStr == expectedStr || (extracted is Number && extracted.toDouble() == expectedStr.toDoubleOrNull())
                    pql.model.PqlOperator.NEQ -> extractedStr != expectedStr
                    pql.model.PqlOperator.GT -> (extracted as? Number)?.toDouble() ?: extractedStr.toDoubleOrNull() ?: 0.0 > (expectedStr.toDoubleOrNull() ?: 0.0)
                    pql.model.PqlOperator.LT -> (extracted as? Number)?.toDouble() ?: extractedStr.toDoubleOrNull() ?: 0.0 < (expectedStr.toDoubleOrNull() ?: 0.0)
                    pql.model.PqlOperator.GE -> (extracted as? Number)?.toDouble() ?: extractedStr.toDoubleOrNull() ?: 0.0 >= (expectedStr.toDoubleOrNull() ?: 0.0)
                    pql.model.PqlOperator.LE -> (extracted as? Number)?.toDouble() ?: extractedStr.toDoubleOrNull() ?: 0.0 <= (expectedStr.toDoubleOrNull() ?: 0.0)
                    pql.model.PqlOperator.IN -> {
                        val list = cond.value as? List<*> ?: return false
                        list.map { it.toString().removeSurrounding("'").removeSurrounding("\"") }.contains(extractedStr)
                    }
                    pql.model.PqlOperator.NOT_IN -> {
                        val list = cond.value as? List<*> ?: return false
                        !list.map { it.toString().removeSurrounding("'").removeSurrounding("\"") }.contains(extractedStr)
                    }
                    pql.model.PqlOperator.LIKE -> {
                        val pattern = expectedStr.replace("%", ".*")
                        extractedStr.matches(Regex(".*$pattern.*"))
                    }
                    pql.model.PqlOperator.MATCHES -> {
                        try {
                            val regex = Regex(expectedStr)
                            extractedStr.matches(regex) || regex.containsMatchIn(extractedStr)
                        } catch (e: Exception) { false }
                    }
                    else -> false
                }
            }
            is PqlCondition.And -> return cond.conditions.all { evaluateConditionInMemory(doc, it, targetScope) }
            is PqlCondition.Or -> return cond.conditions.any { evaluateConditionInMemory(doc, it, targetScope) }
            is PqlCondition.Not -> return !evaluateConditionInMemory(doc, cond.condition, targetScope)
            is PqlCondition.AlwaysFalse -> return false
        }
        return true
    }

    private fun fetchDocsByIdsChunked(ids: Set<String>): List<JsonObject> {
        val result = mutableListOf<JsonObject>()
        if (ids.isEmpty()) return result

        ids.chunked(500).forEach { chunk ->
            val query = JsonObject().apply {
                add("selector", JsonObject().apply {
                    add("_id", JsonObject().apply {
                        val inArr = JsonArray()
                        chunk.forEach { inArr.add(it) }
                        add("\$in", inArr)
                    })
                })
                addProperty("limit", chunk.size)
            }
            dbManager.findDocsStream(dbName, query) { doc -> result.add(doc) }
        }
        return result
    }

    private fun fetchParentsMap(rawResult: List<JsonObject>, query: PqlQuery): Map<String, JsonObject> {
        val traceIds = mutableSetOf<String>()
        val logIds = mutableSetOf<String>()

        val needsTrace = query.projections.any { it.scope == PqlScope.TRACE || it.raw.startsWith("t:") } ||
                query.orderBy.any { it.scope == PqlScope.TRACE } ||
                query.groupBy.any { it.scope == PqlScope.TRACE } ||
                query.selectAll

        val needsLog = query.projections.any { it.scope == PqlScope.LOG || it.raw.startsWith("l:") } ||
                query.orderBy.any { it.scope == PqlScope.LOG } ||
                query.groupBy.any { it.scope == PqlScope.LOG } ||
                query.selectAll

        if (needsTrace || needsLog) {
            rawResult.forEach { obj ->
                if (needsTrace) obj.get("traceId")?.takeIf { !it.isJsonNull }?.asString?.let { traceIds.add(it) }
                if (needsLog) obj.get("logId")?.takeIf { !it.isJsonNull }?.asString?.let { logIds.add(it) }
            }
        }

        val parentsMap = mutableMapOf<String, JsonObject>()
        fetchDocsByIdsChunked(traceIds).forEach { parentsMap[it.get("_id").asString] = it }
        fetchDocsByIdsChunked(logIds).forEach { parentsMap[it.get("_id").asString] = it }
        return parentsMap
    }

    private fun validateFields(docs: List<JsonObject>, query: PqlQuery, parentsMap: Map<String, JsonObject>) {
        val checkFields = query.orderBy.map { it.scope to it.attribute } + query.groupBy.map { it.scope to it.attribute }
        for ((scope, attr) in checkFields) {
            val resolvedAttr = fieldMapper.getStandardName(attr)
            val cleanAttr = resolvedAttr.removePrefix("^^").removePrefix("^")

            if (cleanAttr.isEmpty() || cleanAttr.lowercase().matches(Regex(".*(count|min|max|avg|sum)\\(.*\\)"))) continue

            val collectionLevel = getScopeLevel(query.collection)
            val attrLevel = getScopeLevel(scope)
            if (attrLevel > collectionLevel) continue

            val existsInAny = docs.any { doc ->
                var v = extractRobustValue(doc, scope, cleanAttr)
                if (v == null && scope != query.collection) {
                    val pId = if (scope == PqlScope.TRACE) doc.get("traceId")?.takeIf { !it.isJsonNull }?.asString else doc.get("logId")?.takeIf { !it.isJsonNull }?.asString
                    val pDoc = pId?.let { parentsMap[it] }
                    if (pDoc != null) v = extractRobustValue(pDoc, scope, cleanAttr)
                }
                v != null && v.toString() != "null" && v.toString().isNotBlank()
            }

            if (!existsInAny) {
                throw IllegalArgumentException("Classifier or attribute '$attr' not found")
            }
        }
    }

    fun executeDelete(query: PqlDeleteQuery): JsonObject {
        try {
            if (query.conditions.any { it is PqlCondition.AlwaysFalse }) {
                return JsonObject().apply { addProperty("deleted", 0) }
            }

            val targetScope = query.scope ?: PqlScope.EVENT

            val flatConditions = mutableListOf<PqlCondition>()
            fun flatten(c: PqlCondition) {
                if (c is PqlCondition.And) c.conditions.forEach { flatten(it) } else flatConditions.add(c)
            }
            query.conditions.forEach { flatten(it) }

            val crossScopeConditions = flatConditions.filter { getCrossScope(it, targetScope) != null }
            val sameScopeConditions = flatConditions.filter { getCrossScope(it, targetScope) == null }
            var resolvedQuery = query.copy(conditions = sameScopeConditions)

            if (crossScopeConditions.isNotEmpty()) {
                val parentIds = resolveCrossScopeConditions(crossScopeConditions, targetScope)
                if (parentIds.isEmpty()) return JsonObject().apply { addProperty("deleted", 0) }

                val outerScope = crossScopeConditions.firstNotNullOfOrNull { getCrossScope(it, targetScope) }
                val filterField = when {
                    targetScope == PqlScope.EVENT && outerScope == PqlScope.TRACE -> "traceId"
                    targetScope == PqlScope.EVENT && outerScope == PqlScope.LOG -> "logId"
                    targetScope == PqlScope.TRACE && outerScope == PqlScope.LOG -> "logId"
                    else -> "traceId"
                }

                val idCondition = if (parentIds.size == 1) PqlCondition.Simple(targetScope, filterField, pql.model.PqlOperator.EQ, parentIds.first())
                else PqlCondition.Simple(targetScope, filterField, pql.model.PqlOperator.IN, parentIds)
                resolvedQuery = query.copy(conditions = sameScopeConditions + idCondition)
            }

            val safeConditions = resolvedQuery.conditions.filter { !it.toString().contains("[") }
            val mangoQuery: JsonObject = translator.translateDelete(resolvedQuery.copy(conditions = safeConditions))

            val hasCrossScopeOrderBy = resolvedQuery.orderBy.any { it.scope != targetScope }
            val usesMemoryProcessing = hasCrossScopeOrderBy || resolvedQuery.orderBy.isNotEmpty() || resolvedQuery.limit != null || resolvedQuery.offset != null

            if (!mangoQuery.has("limit") || usesMemoryProcessing) {
                mangoQuery.addProperty("limit", 1000000)
                mangoQuery.remove("skip")
            }

            var rawResultList = mutableListOf<JsonObject>()
            dbManager.findDocsStream(dbName, mangoQuery) { doc ->
                if (resolvedQuery.conditions.isEmpty() || evaluateConditionInMemory(doc, PqlCondition.And(resolvedQuery.conditions), targetScope)) {
                    rawResultList.add(doc)
                }
            }

            if (rawResultList.isEmpty()) {
                return JsonObject().apply { addProperty("deleted", 0) }
            }

            if (resolvedQuery.orderBy.isNotEmpty()) {
                val parentsMap = fetchParentsMap(rawResultList, PqlQuery(collection = targetScope, projections = emptyList(), orderBy = resolvedQuery.orderBy))
                val syntheticQuery = PqlQuery(collection = targetScope, projections = emptyList(), orderBy = resolvedQuery.orderBy)
                rawResultList = executeMemoryOrderBy(rawResultList, syntheticQuery, parentsMap).toMutableList()
            }

            var stream = rawResultList.asSequence()
            resolvedQuery.offset?.let { stream = stream.drop(it) }
            resolvedQuery.limit?.let { stream = stream.take(it) }
            rawResultList = stream.toMutableList()

            val docsToDelete = mutableMapOf<String, String>()
            rawResultList.forEach {
                val id = it.get("_id")?.asString
                val rev = it.get("_rev")?.asString
                if (id != null && rev != null) docsToDelete[id] = rev
            }

            if (targetScope == PqlScope.LOG || targetScope == PqlScope.TRACE) {
                val logIds = mutableSetOf<String>()
                val traceIds = mutableSetOf<String>()

                rawResultList.forEach { doc ->
                    if (targetScope == PqlScope.LOG) {
                        doc.get("_id")?.asString?.let { logIds.add(it) }
                    } else if (targetScope == PqlScope.TRACE) {
                        doc.get("_id")?.asString?.let { traceIds.add(it) }
                        val tId = doc.get("traceId")?.asString ?: doc.get("t:id")?.asString
                        if (tId != null) traceIds.add(tId)
                    }
                }

                if (logIds.isNotEmpty()) {
                    val childQuery = JsonObject().apply {
                        add("selector", JsonObject().apply {
                            add("logId", JsonObject().apply {
                                val inArr = JsonArray()
                                logIds.forEach { inArr.add(it) }
                                add("\$in", inArr)
                            })
                        })
                        addProperty("limit", 1000000)
                    }
                    dbManager.findDocsStream(dbName, childQuery) { doc ->
                        val id = doc.get("_id")?.asString
                        val rev = doc.get("_rev")?.asString
                        if (id != null && rev != null) docsToDelete[id] = rev
                    }
                }

                if (traceIds.isNotEmpty()) {
                    val childQuery = JsonObject().apply {
                        add("selector", JsonObject().apply {
                            add("traceId", JsonObject().apply {
                                val inArr = JsonArray()
                                traceIds.forEach { inArr.add(it) }
                                add("\$in", inArr)
                            })
                        })
                        addProperty("limit", 1000000)
                    }
                    dbManager.findDocsStream(dbName, childQuery) { doc ->
                        val id = doc.get("_id")?.asString
                        val rev = doc.get("_rev")?.asString
                        if (id != null && rev != null) docsToDelete[id] = rev
                    }
                }
            }

            var deletedCount = 0
            if (docsToDelete.isNotEmpty()) {
                docsToDelete.keys.chunked(500).forEach { chunk ->
                    val idsArray = JsonArray()
                    chunk.forEach { idsArray.add(it) }

                    val deleteQuery = JsonObject().apply {
                        add("selector", JsonObject().apply {
                            add("_id", JsonObject().apply {
                                add("\$in", idsArray)
                            })
                        })
                        addProperty("limit", chunk.size)
                    }

                    try {
                        deletedCount += dbManager.deleteDocs(dbName, deleteQuery)
                    } catch (e: Exception) {
                    }
                }
            }

            return JsonObject().apply {
                addProperty("deleted", deletedCount)
                addProperty("status", "success")
            }

        } catch (e: Throwable) {
            throw e
        }
    }

    fun execute(query: PqlQuery): JsonArray {
        try {
            if (query.conditions.any { it is PqlCondition.AlwaysFalse }) return JsonArray()

            val flatConditions = mutableListOf<PqlCondition>()
            fun flatten(c: PqlCondition) {
                if (c is PqlCondition.And) c.conditions.forEach { flatten(it) } else flatConditions.add(c)
            }
            query.conditions.forEach { flatten(it) }

            val crossScopeConditions = flatConditions.filter { getCrossScope(it, query.collection) != null }
            val sameScopeConditions = flatConditions.filter { getCrossScope(it, query.collection) == null }
            var resolvedQuery = query.copy(conditions = sameScopeConditions)

            if (crossScopeConditions.isNotEmpty()) {
                val parentIds = resolveCrossScopeConditions(crossScopeConditions, query.collection)
                if (parentIds.isEmpty()) return JsonArray()

                val outerScope = crossScopeConditions.firstNotNullOfOrNull { getCrossScope(it, query.collection) }
                val filterField = when {
                    query.collection == PqlScope.EVENT && outerScope == PqlScope.TRACE -> "traceId"
                    query.collection == PqlScope.EVENT && outerScope == PqlScope.LOG -> "logId"
                    query.collection == PqlScope.TRACE && outerScope == PqlScope.LOG -> "logId"
                    else -> "traceId"
                }

                val idCondition = if (parentIds.size == 1) PqlCondition.Simple(query.collection, filterField, pql.model.PqlOperator.EQ, parentIds.first())
                else PqlCondition.Simple(query.collection, filterField, pql.model.PqlOperator.IN, parentIds)
                resolvedQuery = query.copy(conditions = sameScopeConditions + idCondition)
            }

            val hasAggs = resolvedQuery.projections.any { hasAggregation(it) } || resolvedQuery.orderBy.any { hasAggregation(it) }
            val isGrouped = resolvedQuery.groupBy.isNotEmpty() || hasAggs
            val hasCrossScopeOrderBy = resolvedQuery.orderBy.any { it.scope != resolvedQuery.collection }

            var rawResultList: List<JsonObject> = emptyList()

            val safeConditions = resolvedQuery.conditions.filter { !it.toString().contains("[") }
            val mangoQuery: JsonObject = translator.translate(resolvedQuery.copy(conditions = safeConditions))

            val hasScopedLimitsOrOffsets = resolvedQuery.limits.isNotEmpty() || resolvedQuery.offsets.isNotEmpty()
            val usesMemoryProcessing = hasCrossScopeOrderBy || isGrouped || resolvedQuery.orderBy.isNotEmpty() || hasScopedLimitsOrOffsets

            val res = mutableListOf<JsonObject>()

            if (!mangoQuery.has("limit") || usesMemoryProcessing) {
                mangoQuery.addProperty("limit", 1000000)
                mangoQuery.remove("skip")

                dbManager.findDocsStream(dbName, mangoQuery) { doc ->
                    if (resolvedQuery.conditions.isEmpty() || evaluateConditionInMemory(doc, PqlCondition.And(resolvedQuery.conditions), resolvedQuery.collection)) {
                        res.add(doc)
                    }
                }
                rawResultList = res
            } else {
                dbManager.findDocsStream(dbName, mangoQuery) { doc ->
                    if (resolvedQuery.conditions.isEmpty() || evaluateConditionInMemory(doc, PqlCondition.And(resolvedQuery.conditions), resolvedQuery.collection)) {
                        res.add(doc)
                    }
                }
                rawResultList = res
            }

            val parentsMap = fetchParentsMap(rawResultList, resolvedQuery)

            if (rawResultList.isNotEmpty()) {
                validateFields(rawResultList, resolvedQuery, parentsMap)
            }

            if (resolvedQuery.projections.any { it.arithmeticExpression != null || (it.function != null && it.function !is pql.model.PqlFunction.AggregationFunction) || (it.function == null && it.arithmeticExpression == null && it.raw.matches(Regex(".*[\\+\\-\\*/].*")) && !it.raw.contains("http")) }) {
                rawResultList = applyArithmeticExpressions(rawResultList, resolvedQuery)
            }

            val hasAggs2 = resolvedQuery.projections.any { hasAggregation(it) } || resolvedQuery.orderBy.any { hasAggregation(it) }
            val isGrouped2 = resolvedQuery.groupBy.isNotEmpty() || hasAggs2

            if (resolvedQuery.orderBy.isNotEmpty() && !isGrouped2) {
                rawResultList = executeMemoryOrderBy(rawResultList, resolvedQuery, parentsMap)
            }

            rawResultList = applyHierarchicalLimitsAndOffsets(rawResultList, resolvedQuery, applyGlobal = !isGrouped2)

            if (isGrouped2) {
                val jsonArray = JsonArray().apply { rawResultList.forEach { add(it) } }
                return formatGroupedResponse(jsonArray, resolvedQuery, parentsMap)
            }

            val finalArray = JsonArray().apply { rawResultList.forEach { add(it) } }
            injectParentsForFlatResults(finalArray)
            return formatter.format(finalArray, resolvedQuery)

        } catch (e: Throwable) {
            throw e
        }
    }

    private fun applyArithmeticExpressions(rawDocs: List<JsonObject>, query: PqlQuery): List<JsonObject> {
        val enrichedDocs = mutableListOf<JsonObject>()
        for (doc in rawDocs) {
            query.projections.forEach { proj ->
                val isMathStr = proj.function == null && proj.arithmeticExpression == null && proj.raw.matches(Regex(".*[\\+\\-\\*/].*")) && !proj.raw.contains("http")
                if (proj.arithmeticExpression != null || (proj.function != null && proj.function !is pql.model.PqlFunction.AggregationFunction) || isMathStr) {
                    val value = if (proj.arithmeticExpression != null) evaluateMath(listOf(doc), proj.arithmeticExpression!!, emptyMap(), emptyMap())
                    else if (proj.function != null) evaluateFunction(listOf(doc), proj.function!!, emptyMap(), emptyMap())
                    else evaluateSimpleMathString(doc, proj.scope ?: PqlScope.EVENT, proj.raw)
                    doc.addComputedProperty(proj.raw, value)
                }
            }
            enrichedDocs.add(doc)
        }
        return enrichedDocs
    }

    private fun executeMemoryOrderBy(rawResult: List<JsonObject>, query: PqlQuery, parentsMap: Map<String, JsonObject>): List<JsonObject> {
        return rawResult.sortedWith(Comparator { a, b ->
            var cmp = 0
            for (order in query.orderBy) {
                var valA = extractRobustValue(a, order.scope, order.attribute)
                if (valA == null && order.scope != query.collection) {
                    val pId = if (order.scope == PqlScope.TRACE) a.get("traceId")?.takeIf { !it.isJsonNull }?.asString else a.get("logId")?.takeIf { !it.isJsonNull }?.asString
                    val pDoc = pId?.let { parentsMap[it] }
                    if (pDoc != null) valA = extractRobustValue(pDoc, order.scope, order.attribute)
                }

                var valB = extractRobustValue(b, order.scope, order.attribute)
                if (valB == null && order.scope != query.collection) {
                    val pId = if (order.scope == PqlScope.TRACE) b.get("traceId")?.takeIf { !it.isJsonNull }?.asString else b.get("logId")?.takeIf { !it.isJsonNull }?.asString
                    val pDoc = pId?.let { parentsMap[it] }
                    if (pDoc != null) valB = extractRobustValue(pDoc, order.scope, order.attribute)
                }

                val numA = valA?.toString()?.toDoubleOrNull()
                val numB = valB?.toString()?.toDoubleOrNull()
                val finalValA = numA ?: valA
                val finalValB = numB ?: valB

                cmp = compareExtractedValues(finalValA, finalValB)
                if (order.direction == SortDirection.DESC) cmp = -cmp
                if (cmp != 0) break
            }
            cmp
        })
    }

    private fun applyHierarchicalLimitsAndOffsets(docs: List<JsonObject>, query: PqlQuery, applyGlobal: Boolean): List<JsonObject> {
        val logLimit = query.limits[PqlScope.LOG]
        val logOffset = query.offsets[PqlScope.LOG] ?: 0
        val traceLimit = query.limits[PqlScope.TRACE]
        val traceOffset = query.offsets[PqlScope.TRACE] ?: 0
        val eventLimit = query.limits[PqlScope.EVENT]
        val eventOffset = query.offsets[PqlScope.EVENT] ?: 0

        val globalLimit = if (applyGlobal) query.limit ?: Int.MAX_VALUE else Int.MAX_VALUE
        val globalOffset = if (applyGlobal) query.offset ?: 0 else 0

        if (logLimit == null && logOffset == 0 && traceLimit == null && traceOffset == 0 &&
            eventLimit == null && eventOffset == 0 && globalLimit == Int.MAX_VALUE && globalOffset == 0) {
            return docs
        }

        val logCounter = mutableMapOf<String, Int>()
        val traceCounter = mutableMapOf<String, Int>()
        val eventCounter = mutableMapOf<String, Int>()
        val logSeen = mutableSetOf<String>()
        val traceSeen = mutableSetOf<String>()
        val result = mutableListOf<JsonObject>()

        var globalCount = 0
        var globalSkipped = 0

        for (doc in docs) {
            val docType = doc.get("docType")?.takeIf { !it.isJsonNull }?.asString?.intern() ?: "event"
            val lId = if (docType == "log") doc.get("_id")?.takeIf { !it.isJsonNull }?.asString?.intern() ?: "unknown" else doc.get("logId")?.takeIf { !it.isJsonNull }?.asString?.intern() ?: "unknown"
            val tId = if (docType == "trace") doc.get("_id")?.takeIf { !it.isJsonNull }?.asString?.intern() ?: "unknown" else doc.get("traceId")?.takeIf { !it.isJsonNull }?.asString?.intern() ?: doc.get("originalTraceId")?.takeIf { !it.isJsonNull }?.asString?.intern() ?: "unknown"
            val logTraceKey = "$lId|$tId"

            if (lId !in logSeen) {
                logSeen.add(lId)
                logCounter[lId] = logSeen.size - 1
            }
            val lIndex = logCounter[lId]!!
            if (lIndex < logOffset) continue
            if (logLimit != null && lIndex >= logOffset + logLimit) continue

            if (logTraceKey !in traceSeen) {
                traceSeen.add(logTraceKey)
                traceCounter[lId] = traceCounter.getOrDefault(lId, 0) + 1
            }
            val tIndex = traceCounter[lId]!! - 1
            if (tIndex < traceOffset) continue
            if (traceLimit != null && tIndex >= traceOffset + traceLimit) continue

            val eIndex = eventCounter.getOrDefault(logTraceKey, 0)
            if (eIndex < eventOffset) {
                eventCounter[logTraceKey] = eIndex + 1
                continue
            }
            if (eventLimit != null && eIndex >= eventOffset + eventLimit) continue

            eventCounter[logTraceKey] = eIndex + 1

            if (globalSkipped < globalOffset) {
                globalSkipped++
                continue
            }
            if (globalCount >= globalLimit) break

            result.add(doc)
            globalCount++
        }
        return result
    }

    private fun injectParentsForFlatResults(docs: JsonArray) {
        if (docs.size() == 0) return
        val traceIds = mutableSetOf<String>()
        val logIds = mutableSetOf<String>()
        docs.forEach {
            if (it.isJsonObject) {
                val obj = it.asJsonObject
                obj.get("traceId")?.takeIf { !it.isJsonNull }?.asString?.let { tId -> traceIds.add(tId.intern()) }
                obj.get("logId")?.takeIf { !it.isJsonNull }?.asString?.let { lId -> logIds.add(lId.intern()) }
                obj.get("l:id")?.takeIf { !it.isJsonNull }?.asString?.let { lId -> logIds.add(lId.intern()) }
            }
        }

        val traceDocsMap = mutableMapOf<String, JsonObject>()
        fetchDocsByIdsChunked(traceIds).forEach { traceDocsMap[it.get("_id").asString] = it }

        val logDocsMap = mutableMapOf<String, JsonObject>()
        fetchDocsByIdsChunked(logIds).forEach { logDocsMap[it.get("_id").asString] = it }

        docs.forEach {
            if (it.isJsonObject) {
                val obj = it.asJsonObject
                val tId = obj.get("traceId")?.takeIf { !it.isJsonNull }?.asString
                val lId = obj.get("logId")?.takeIf { !it.isJsonNull }?.asString ?: obj.get("l:id")?.takeIf { !it.isJsonNull }?.asString
                injectFullTraceAttributes(obj, traceDocsMap[tId])
                injectFullLogAttributes(obj, logDocsMap[lId])
            }
        }
    }

    private fun injectFullTraceAttributes(eventObj: JsonObject, traceDoc: JsonObject?) {
        if (traceDoc == null) return
        val traceName = traceDoc.get("identity:id")?.takeIf { !it.isJsonNull }?.asString
            ?: traceDoc.getAsJsonObject("xes_attributes")?.get("concept:name")?.takeIf { !it.isJsonNull }?.asString
            ?: traceDoc.get("originalTraceId")?.takeIf { !it.isJsonNull }?.asString

        if (traceName != null) {
            eventObj.addProperty("t:name", traceName)
            eventObj.addProperty("t:concept:name", traceName)
        }

        val traceId = traceDoc.get("identity:id")?.takeIf { !it.isJsonNull }?.asString
        if (traceId != null) eventObj.addProperty("t:id", traceId)

        traceDoc.getAsJsonObject("xes_attributes")?.entrySet()?.forEach { (k, v) ->
            val pKey = getTPrefix(k)
            if (!eventObj.has(pKey)) eventObj.add(pKey, v)
        }
    }

    private fun injectFullLogAttributes(eventObj: JsonObject, logDoc: JsonObject?) {
        if (logDoc == null) return
        val logName = logDoc.getAsJsonObject("log_attributes")?.get("concept:name")?.takeIf { !it.isJsonNull }?.asString
            ?: logDoc.get("identity:id")?.takeIf { !it.isJsonNull }?.asString

        if (logName != null) {
            eventObj.addProperty("l:name", logName)
            eventObj.addProperty("l:concept:name", logName)
        }

        val logId = logDoc.get("identity:id")?.takeIf { !it.isJsonNull }?.asString
        if (logId != null) eventObj.addProperty("l:id", logId)

        logDoc.getAsJsonObject("log_attributes")?.entrySet()?.forEach { (k, v) ->
            val pKey = getLPrefix(k)
            if (!eventObj.has(pKey)) eventObj.add(pKey, v)
        }
    }

    private fun formatGroupedResponse(docs: JsonArray, query: PqlQuery, parentsMap: Map<String, JsonObject>): JsonArray {
        val hoistedEventGroupField = query.groupBy.firstOrNull { it.attribute.startsWith("^") && !it.attribute.startsWith("^^") }
        val isImplicitGlobal = query.groupBy.isEmpty() && query.projections.any { hasAggregation(it) } && !query.projections.any { it.function is pql.model.PqlFunction.AggregationFunction && it.function.attribute?.startsWith("^^") == true }

        if (isImplicitGlobal) return formatSingleGlobalGroup(docs, query)

        return if (hoistedEventGroupField != null) {
            formatVariantGroupedResponse(docs, query, hoistedEventGroupField)
        } else {
            formatFlatGroupedResponse(docs, query)
        }
    }

    private fun formatSingleGlobalGroup(docs: JsonArray, query: PqlQuery): JsonArray {
        val groupResult = JsonObject()
        groupResult.addProperty("count", docs.size())

        if (docs.size() == 0) return JsonArray().apply { add(groupResult) }

        val representative = docs[0].asJsonObject

        val repCopy = JsonObject()
        for ((key, value) in representative.entrySet()) {
            repCopy.add(key, value)
        }

        injectParentsForFlatResults(JsonArray().apply { add(repCopy) })

        val formattedRep = formatter.format(JsonArray().apply { add(repCopy) }, query)[0].asJsonObject

        val traceDocsMap = mutableMapOf<String, JsonObject>()
        val logDocsMap = mutableMapOf<String, JsonObject>()

        val eventsInGroup = mutableListOf<JsonObject>()
        for (i in 0 until docs.size()) eventsInGroup.add(docs[i].asJsonObject)

        val traceIds = eventsInGroup.mapNotNull { it.get("traceId")?.takeIf { it1 -> !it1.isJsonNull }?.asString }.toSet()
        fetchDocsByIdsChunked(traceIds).forEach { traceDocsMap[it.get("_id").asString] = it }

        query.projections.forEach { proj ->
            if (proj.function != null || proj.arithmeticExpression != null) {
                val value = evaluateGroupProjection(eventsInGroup, proj, traceDocsMap, logDocsMap)
                formattedRep.addComputedProperty(proj.raw, value)
                groupResult.addComputedProperty(proj.raw, value)
            }
        }

        val representativeEvents = JsonArray().apply { add(formattedRep) }
        groupResult.add("events", representativeEvents)

        return JsonArray().apply { add(groupResult) }
    }

    private fun formatFlatGroupedResponse(docs: JsonArray, query: PqlQuery): JsonArray {
        val traceIds = mutableSetOf<String>()
        val logIds = mutableSetOf<String>()
        docs.forEach {
            if (it.isJsonObject) {
                val obj = it.asJsonObject
                obj.get("traceId")?.takeIf { !it.isJsonNull }?.asString?.let { tId -> traceIds.add(tId.intern()) }
                obj.get("logId")?.takeIf { !it.isJsonNull }?.asString?.let { lId -> logIds.add(lId.intern()) }
            }
        }

        val traceDocsMap = mutableMapOf<String, JsonObject>()
        fetchDocsByIdsChunked(traceIds).forEach { traceDocsMap[it.get("_id").asString] = it }

        val logDocsMap = mutableMapOf<String, JsonObject>()
        fetchDocsByIdsChunked(logIds).forEach { logDocsMap[it.get("_id").asString] = it }

        val flatGroups = mutableMapOf<String, MutableList<JsonObject>>()

        val hasImplicitGlobalHoisting = query.groupBy.isEmpty() && query.projections.any { it.function is pql.model.PqlFunction.AggregationFunction && it.function.attribute?.startsWith("^^") == true }

        val needsPerTraceGrouping = !hasImplicitGlobalHoisting && query.groupBy.none { it.attribute.startsWith("^") }

        for (element in docs) {
            if (!element.isJsonObject) continue
            val doc = element.asJsonObject
            val tId = doc.get("traceId")?.takeIf { !it.isJsonNull }?.asString?.intern() ?: "unknown"
            val tracePrefix = if (needsPerTraceGrouping) "$tId|" else ""

            val groupKey = if (query.groupBy.isEmpty()) {
                if (hasImplicitGlobalHoisting) "GLOBAL_GROUP" else tracePrefix
            } else {
                tracePrefix + query.groupBy.joinToString("|") { gb ->
                    val lId = doc.get("logId")?.takeIf { !it.isJsonNull }?.asString?.intern()
                    val targetDoc = when (gb.scope) {
                        PqlScope.LOG -> if (lId != null) (logDocsMap[lId] ?: doc) else doc
                        PqlScope.TRACE -> traceDocsMap[tId] ?: doc
                        else -> doc
                    }

                    val cleanAttr = gb.attribute.removePrefix("^^").removePrefix("^")
                    val extracted = extractRobustValue(targetDoc, gb.scope, cleanAttr)
                    extracted?.toString() ?: "null"
                }
            }

            flatGroups.getOrPut(groupKey.intern()) { mutableListOf() }.add(doc)
        }

        val finalDocsTemp = mutableListOf<Pair<JsonObject, JsonObject>>()

        flatGroups.forEach { (_, eventsInGroup) ->
            val groupResult = JsonObject()
            groupResult.addProperty("count", eventsInGroup.size)

            query.projections.forEach { proj ->
                if (proj.function != null || proj.arithmeticExpression != null) {
                    val value = evaluateGroupProjection(eventsInGroup, proj, traceDocsMap, logDocsMap)
                    groupResult.addComputedProperty(proj.raw, value)
                }
            }

            query.orderBy.forEach { order ->
                if (order.function != null || order.arithmeticExpression != null) {
                    val value = if (order.function is pql.model.PqlFunction.AggregationFunction) {
                        evaluateAggregation(eventsInGroup, order.function, traceDocsMap, logDocsMap)
                    } else if (order.arithmeticExpression != null) {
                        evaluateMath(eventsInGroup, order.arithmeticExpression, traceDocsMap, logDocsMap)
                    } else null
                    groupResult.addComputedProperty(order.attribute, value)
                }
            }

            finalDocsTemp.add(Pair(groupResult, eventsInGroup.first()))
        }

        if (query.orderBy.isNotEmpty()) {
            finalDocsTemp.sortWith(Comparator { a, b ->
                var cmp = 0
                val groupA = a.first
                val groupB = b.first
                val repA = a.second
                val repB = b.second

                for (order in query.orderBy) {
                    val keyRaw = order.attribute
                    val keyClean = keyRaw.replace(" ", "")

                    var valA: Any? = groupA.get(keyRaw)?.let { if (it.isJsonPrimitive && it.asJsonPrimitive.isNumber) it.asDouble else if (!it.isJsonNull) it.asString else null }
                    if (valA == null) valA = groupA.get(keyClean)?.let { if (it.isJsonPrimitive && it.asJsonPrimitive.isNumber) it.asDouble else if (!it.isJsonNull) it.asString else null }
                    if (valA == null) valA = extractRobustValue(repA, order.scope, order.attribute)

                    var valB: Any? = groupB.get(keyRaw)?.let { if (it.isJsonPrimitive && it.asJsonPrimitive.isNumber) it.asDouble else if (!it.isJsonNull) it.asString else null }
                    if (valB == null) valB = groupB.get(keyClean)?.let { if (it.isJsonPrimitive && it.asJsonPrimitive.isNumber) it.asDouble else if (!it.isJsonNull) it.asString else null }
                    if (valB == null) valB = extractRobustValue(repB, order.scope, order.attribute)

                    val numA = valA?.toString()?.toDoubleOrNull()
                    val numB = valB?.toString()?.toDoubleOrNull()

                    val finalValA = numA ?: valA
                    val finalValB = numB ?: valB

                    cmp = compareExtractedValues(finalValA, finalValB)
                    if (order.direction == SortDirection.DESC) cmp = -cmp
                    if (cmp != 0) break
                }
                cmp
            })
        }

        var stream = finalDocsTemp.asSequence()
        query.offset?.let { stream = stream.drop(it) }
        query.limit?.let { stream = stream.take(it) }

        val sortedResult = JsonArray()
        val filteredStream = stream.toList()

        filteredStream.chunked(250).forEach { chunk ->
            val unformattedEvents = JsonArray()

            chunk.forEach { (_, representative) ->
                val repCopy = JsonObject()
                for ((key, value) in representative.entrySet()) {
                    repCopy.add(key, value)
                }

                unformattedEvents.add(repCopy)
            }

            val formattedChunk = if (unformattedEvents.size() > 0) formatter.format(unformattedEvents, query) else JsonArray()

            chunk.forEachIndexed { index, (groupResult, _) ->
                val formattedRep = formattedChunk[index].asJsonObject

                for ((k, v) in groupResult.entrySet()) {
                    if (k != "count" && k != "events") {
                        formattedRep.add(k, v)
                    }
                }

                val representativeEvents = JsonArray().apply { add(formattedRep) }
                groupResult.add("events", representativeEvents)
                sortedResult.add(groupResult)
            }
        }
        return sortedResult
    }

    private fun formatVariantGroupedResponse(docs: JsonArray, query: PqlQuery, hoistedGroupField: pql.model.PqlGroupByField): JsonArray {
        val attrName = hoistedGroupField.attribute.removePrefix("^").removePrefix("e:")
        val eventsByTrace = mutableMapOf<String, MutableList<JsonObject>>()

        for (element in docs) {
            if (!element.isJsonObject) continue
            val doc = element.asJsonObject
            val traceId = doc.get("traceId")?.takeIf { !it.isJsonNull }?.asString?.intern() ?: "unknown"
            eventsByTrace.getOrPut(traceId) { mutableListOf() }.add(doc)
        }

        val innerSortRule = query.orderBy.firstOrNull { it.scope == PqlScope.EVENT && !it.attribute.lowercase().startsWith("count") && !it.attribute.matches(Regex(".*(?:min|max|avg|sum|count)\\(.*\\).*")) }
        val outerSortRule = query.orderBy.firstOrNull { order ->
            val attr = order.attribute.lowercase()
            attr.startsWith("count") || attr.startsWith("min") || attr.startsWith("max") || attr.startsWith("avg") || attr.startsWith("sum")
        }
        val traceGroupKeys = query.groupBy.filter { !it.attribute.startsWith("^") && it.scope != PqlScope.EVENT }

        val validTraceIds = eventsByTrace.keys.filter { it != "unknown" }.toSet()
        val traceDocsMap = mutableMapOf<String, JsonObject>()

        fetchDocsByIdsChunked(validTraceIds).forEach { doc ->
            val tId = doc.get("_id")?.asString
            if (tId != null) traceDocsMap[tId.intern()] = doc
        }

        val logDocsMap = mutableMapOf<String, JsonObject>()
        val logIds = validTraceIds.mapNotNull { eventsByTrace[it]?.firstOrNull()?.get("logId")?.takeIf { l -> !l.isJsonNull }?.asString?.intern() }.toSet()

        fetchDocsByIdsChunked(logIds).forEach { doc ->
            val lId = doc.get("_id")?.asString
            if (lId != null) logDocsMap[lId.intern()] = doc
        }

        val traceGroups = mutableMapOf<String, MutableList<List<JsonObject>>>()

        for ((tId, events) in eventsByTrace) {
            events.sortWith(Comparator { a, b ->
                var cmp = 0
                if (innerSortRule != null) {
                    val valA = extractRobustValue(a, innerSortRule.scope, innerSortRule.attribute)
                    val valB = extractRobustValue(b, innerSortRule.scope, innerSortRule.attribute)
                    cmp = compareExtractedValues(valA, valB)
                    if (innerSortRule.direction == SortDirection.DESC) cmp = -cmp
                } else {
                    val idxA = a.get("eventIndex")?.takeIf { !it.isJsonNull }?.asInt ?: 0
                    val idxB = b.get("eventIndex")?.takeIf { !it.isJsonNull }?.asInt ?: 0
                    cmp = idxA.compareTo(idxB)
                }

                if (cmp == 0) {
                    val tsA = extractTimestamp(a)
                    val tsB = extractTimestamp(b)
                    cmp = tsA.compareTo(tsB)
                }
                cmp
            })

            var prefixKey = ""
            if (traceGroupKeys.isNotEmpty() && events.isNotEmpty()) {
                val representative = events.first()
                prefixKey = traceGroupKeys.joinToString("|") { gb ->
                    val lId = representative.get("logId")?.takeIf { !it.isJsonNull }?.asString
                    val targetDoc = when (gb.scope) {
                        PqlScope.LOG -> if (lId != null) (logDocsMap[lId] ?: representative) else representative
                        PqlScope.TRACE -> traceDocsMap[tId] ?: representative
                        else -> representative
                    }
                    val extracted = extractRobustValue(targetDoc, gb.scope, gb.attribute)
                    extracted?.toString() ?: "null"
                } + "|"
            }

            val sequenceKey = prefixKey + events.joinToString(",") { evt ->
                extractRobustValue(evt, PqlScope.EVENT, attrName)?.toString() ?: "null"
            }

            traceGroups.getOrPut(sequenceKey.intern()) { mutableListOf() }.add(events)
        }

        val finalDocsTemp = mutableListOf<Pair<JsonObject, List<JsonObject>>>()

        traceGroups.forEach { (_, listOfTraces) ->
            val groupResult = JsonObject()

            val sequenceArray = JsonArray()
            listOfTraces.first().forEach { evt ->
                val v = extractRobustValue(evt, PqlScope.EVENT, attrName)
                if (v is Number) sequenceArray.add(v)
                else if (v is Boolean) sequenceArray.add(v)
                else sequenceArray.add(v?.toString() ?: "null")
            }

            groupResult.add(hoistedGroupField.attribute, sequenceArray)
            groupResult.addProperty("count", listOfTraces.size)

            val allEventsInGroup = listOfTraces.flatten()
            query.projections.forEach { proj ->
                if (proj.function != null || proj.arithmeticExpression != null) {
                    val value = evaluateGroupProjection(allEventsInGroup, proj, traceDocsMap, logDocsMap)
                    groupResult.addComputedProperty(proj.raw, value)
                }
            }

            if (outerSortRule != null) {
                if (outerSortRule.function != null || outerSortRule.arithmeticExpression != null) {
                    val isJustCount = outerSortRule.attribute.lowercase().startsWith("count")
                    if (!isJustCount) {
                        val allEvents = listOfTraces.flatten()
                        val value = if (outerSortRule.function is pql.model.PqlFunction.AggregationFunction) {
                            evaluateAggregation(allEvents, outerSortRule.function, traceDocsMap, logDocsMap)
                        } else if (outerSortRule.arithmeticExpression != null) {
                            evaluateMath(allEvents, outerSortRule.arithmeticExpression, traceDocsMap, logDocsMap)
                        } else null

                        if (value != null) {
                            when (value) {
                                is Number -> groupResult.addProperty(outerSortRule.attribute, value)
                                is Boolean -> groupResult.addProperty(outerSortRule.attribute, value)
                                else -> groupResult.addProperty(outerSortRule.attribute, value.toString())
                            }
                        }
                    }
                }
            }
            finalDocsTemp.add(Pair(groupResult, listOfTraces.first()))
        }

        if (outerSortRule != null) {
            finalDocsTemp.sortWith(Comparator { a, b ->
                var valA: Any? = a.first.get(outerSortRule.attribute)?.let { if (it.isJsonPrimitive && it.asJsonPrimitive.isNumber) it.asDouble else if (!it.isJsonNull) it.asString else null }
                var valB: Any? = b.first.get(outerSortRule.attribute)?.let { if (it.isJsonPrimitive && it.asJsonPrimitive.isNumber) it.asDouble else if (!it.isJsonNull) it.asString else null }

                if (outerSortRule.attribute.lowercase().startsWith("count")) {
                    valA = a.first.get("count")?.asInt
                    valB = b.first.get("count")?.asInt
                }

                if (valA == null) valA = a.first.get("count")?.asInt
                if (valB == null) valB = b.first.get("count")?.asInt

                var cmp = compareExtractedValues(valA, valB)
                if (outerSortRule.direction == SortDirection.DESC) cmp = -cmp
                cmp
            })
        } else {
            finalDocsTemp.sortByDescending { it.first.get("count").asInt }
        }

        var stream = finalDocsTemp.asSequence()
        query.offset?.let { stream = stream.drop(it) }
        query.limit?.let { stream = stream.take(it) }

        val sortedResult = JsonArray()
        val filteredStream = stream.toList()

        filteredStream.chunked(50).forEach { chunk ->
            val unformattedEvents = JsonArray()
            val variantEventCounts = mutableListOf<Int>()

            chunk.forEach { (_, firstTraceEvents) ->
                var count = 0
                firstTraceEvents.forEach { eventDoc ->
                    val repCopy = JsonObject()
                    for ((key, value) in eventDoc.entrySet()) {
                        repCopy.add(key, value)
                    }
                    unformattedEvents.add(repCopy)
                    count++
                }
                variantEventCounts.add(count)
            }

            val allFormattedEvents = if (unformattedEvents.size() > 0) formatter.format(unformattedEvents, query) else JsonArray()

            var eventIndex = 0
            chunk.forEachIndexed { index, (groupResult, _) ->
                val count = variantEventCounts[index]
                val representativeEvents = JsonArray()

                for (i in 0 until count) {
                    representativeEvents.add(allFormattedEvents[eventIndex++])
                }

                groupResult.add("events", representativeEvents)
                sortedResult.add(groupResult)
            }
        }
        return sortedResult
    }

    private fun extractTimestamp(doc: JsonObject): String {
        return doc.get("timestamp")?.takeIf { !it.isJsonNull }?.asString?.intern()
            ?: doc.getAsJsonObject("xes_attributes")?.get("time:timestamp")?.takeIf { !it.isJsonNull }?.asString?.intern()
            ?: ""
    }

    private fun evaluateGroupProjection(
        events: List<JsonObject>,
        proj: pql.model.PqlProjection,
        traceDocsMap: Map<String, JsonObject>,
        logDocsMap: Map<String, JsonObject>
    ): Any? {
        if (proj.function != null) {
            return evaluateFunction(events, proj.function, traceDocsMap, logDocsMap)
        }
        if (proj.arithmeticExpression != null) {
            return evaluateMath(events, proj.arithmeticExpression, traceDocsMap, logDocsMap)
        }
        return null
    }

    private fun evaluateSimpleMathString(target: JsonObject, scope: PqlScope, expression: String): Double? {
        fun eval(expr: String): Double? {
            val cleanExpr = expr.replace(" ", "")
            if (cleanExpr.contains("(")) {
                var openIdx = -1
                var closeIdx = -1
                var depth = 0
                for (i in cleanExpr.indices) {
                    if (cleanExpr[i] == '(') {
                        if (depth == 0) openIdx = i
                        depth++
                    } else if (cleanExpr[i] == ')') {
                        depth--
                        if (depth == 0) {
                            closeIdx = i
                            break
                        }
                    }
                }
                if (openIdx != -1 && closeIdx != -1) {
                    val inner = cleanExpr.substring(openIdx + 1, closeIdx)
                    val evaluatedInner = eval(inner) ?: return null
                    val newExpr = cleanExpr.substring(0, openIdx) + evaluatedInner.toString() + cleanExpr.substring(closeIdx + 1)
                    return eval(newExpr)
                }
            }

            val addIdx = cleanExpr.lastIndexOf('+')
            val subIdx = cleanExpr.lastIndexOf('-')
            val isSubOperator = subIdx > 0 && cleanExpr[subIdx - 1] !in listOf('+', '-', '*', '/')

            if (addIdx > 0 || isSubOperator) {
                val idx = if (addIdx > (if (isSubOperator) subIdx else -1)) addIdx else subIdx
                val left = eval(cleanExpr.substring(0, idx)) ?: return null
                val right = eval(cleanExpr.substring(idx + 1)) ?: return null
                return if (cleanExpr[idx] == '+') left + right else left - right
            }

            val mulIdx = cleanExpr.lastIndexOf('*')
            val divIdx = cleanExpr.lastIndexOf('/')
            if (mulIdx > 0 || divIdx > 0) {
                val idx = maxOf(mulIdx, divIdx)
                val left = eval(cleanExpr.substring(0, idx)) ?: return null
                val right = eval(cleanExpr.substring(idx + 1)) ?: return null
                return if (cleanExpr[idx] == '*') left * right else if (right != 0.0) left / right else null
            }

            return extractRobustValue(target, scope, cleanExpr)?.toString()?.toDoubleOrNull() ?: cleanExpr.toDoubleOrNull()
        }
        return eval(expression)
    }

    private fun evaluateFunction(
        events: List<JsonObject>,
        func: pql.model.PqlFunction,
        traceDocsMap: Map<String, JsonObject>,
        logDocsMap: Map<String, JsonObject>
    ): Any? {
        if (func is pql.model.PqlFunction.AggregationFunction) {
            return evaluateAggregation(events, func, traceDocsMap, logDocsMap)
        }
        if (func is pql.model.PqlFunction.ScalarFunction1) {
            val scope = extractScopeFromArithmetic(func.expression) ?: PqlScope.EVENT

            val repEvt = events.firstOrNull() ?: return null
            val tId = repEvt.get("traceId")?.takeIf { !it.isJsonNull }?.asString
            val lId = repEvt.get("logId")?.takeIf { !it.isJsonNull }?.asString

            val target = when (scope) {
                PqlScope.LOG -> if (lId != null) logDocsMap[lId] ?: repEvt else repEvt
                PqlScope.TRACE -> if (tId != null) traceDocsMap[tId] ?: repEvt else repEvt
                else -> repEvt
            }

            val ext = evaluateMath(events, func.expression, traceDocsMap, logDocsMap) ?: return null

            val strVal = ext.toString()

            return when (func.type.name.uppercase()) {
                "UPPER" -> strVal.uppercase()
                "LOWER" -> strVal.lowercase()
                "ROUND" -> strVal.toDoubleOrNull()?.let { Math.round(it) }
                "YEAR" -> parseDatePart(strVal, "year")
                "MONTH" -> parseDatePart(strVal, "month")
                "DAY" -> parseDatePart(strVal, "day")
                "HOUR" -> parseDatePart(strVal, "hour")
                "MINUTE" -> parseDatePart(strVal, "minute")
                "SECOND" -> parseDatePart(strVal, "second")
                "MILLISECOND" -> parseDatePart(strVal, "millisecond")
                "QUARTER" -> parseDatePart(strVal, "quarter")
                "DAYOFWEEK" -> parseDatePart(strVal, "dayofweek")
                "DATE" -> parseDatePart(strVal, "date")
                "TIME" -> parseDatePart(strVal, "time")
                else -> strVal
            }
        }
        if (func is pql.model.PqlFunction.ScalarFunction0 && func.type == pql.model.PqlFunction.Scalar0.NOW) {
            return java.time.OffsetDateTime.now().toString()
        }
        return null
    }

    private fun parseDatePart(dateStr: String, part: String): Any? {
        return try {
            val dt = OffsetDateTime.parse(dateStr)
            when (part) {
                "year" -> dt.year
                "month" -> dt.monthValue
                "day" -> dt.dayOfMonth
                "hour" -> dt.hour
                "minute" -> dt.minute
                "second" -> dt.second
                "millisecond" -> dt.nano / 1000000
                "quarter" -> (dt.monthValue - 1) / 3 + 1
                "dayofweek" -> (dt.dayOfWeek.value % 7) + 1
                "date" -> dt.toLocalDate().atStartOfDay().atOffset(dt.offset).toString()
                "time" -> dt.toLocalTime().atDate(java.time.LocalDate.ofEpochDay(0)).atOffset(dt.offset).toString()
                else -> null
            }
        } catch (e: Exception) {
            null
        }
    }

    private fun evaluateAggregation(
        events: List<JsonObject>,
        func: pql.model.PqlFunction.AggregationFunction,
        traceDocsMap: Map<String, JsonObject>,
        logDocsMap: Map<String, JsonObject>
    ): Any? {
        val aggType = func.type
        val scope = func.scope ?: pql.model.PqlScope.EVENT
        val attribute = func.attribute ?: return null

        val cleanAttribute = attribute.removePrefix("^^").removePrefix("^")

        val rawValues = events.mapNotNull { evt ->
            val tId = evt.get("traceId")?.takeIf { !it.isJsonNull }?.asString
            val lId = evt.get("logId")?.takeIf { !it.isJsonNull }?.asString

            val target = when (scope) {
                pql.model.PqlScope.LOG -> if (lId != null) logDocsMap[lId] ?: evt else evt
                pql.model.PqlScope.TRACE -> if (tId != null) traceDocsMap[tId] ?: evt else evt
                else -> evt
            }
            var v = extractRobustValue(target, scope, cleanAttribute)
            if (v == null) {
                v = evaluateSimpleMathString(target, scope, cleanAttribute)
            }
            v
        }

        if (rawValues.isEmpty()) return null

        if (aggType.name == "COUNT") {
            return when (scope) {
                pql.model.PqlScope.LOG -> rawValues.distinct().size.toDouble()
                pql.model.PqlScope.TRACE -> rawValues.distinct().size.toDouble()
                else -> rawValues.size.toDouble()
            }
        }

        val isStringAgg = rawValues.any { it is String && it.toDoubleOrNull() == null } &&
                (cleanAttribute.contains("name") || cleanAttribute.contains("time") || cleanAttribute.contains("timestamp") || cleanAttribute.contains("resource") || cleanAttribute.contains("transition"))

        if (isStringAgg && (aggType.name == "MIN" || aggType.name == "MAX")) {
            val strValues = rawValues.map { it.toString() }
            return if (aggType.name == "MIN") strValues.minOrNull() else strValues.maxOrNull()
        }

        val values = rawValues.mapNotNull { extracted ->
            if (extracted is String && (cleanAttribute.contains("time") || cleanAttribute.contains("timestamp"))) {
                dateParsingCache.getOrPut(extracted) {
                    try {
                        val format = DateTimeFormatter.ISO_DATE_TIME
                        val ta = format.parseBest(extracted, Instant::from, OffsetDateTime::from)
                        if (ta is Instant) ta.toEpochMilli().toDouble() else (ta as OffsetDateTime).toInstant().toEpochMilli().toDouble()
                    } catch(e: Exception) {
                        extracted.toDoubleOrNull() ?: 0.0
                    }
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

    private fun evaluateMath(
        events: List<JsonObject>,
        expr: pql.model.PqlArithmeticExpression,
        traceDocsMap: Map<String, JsonObject>,
        logDocsMap: Map<String, JsonObject>
    ): Any? {
        return when (expr) {
            is pql.model.PqlArithmeticExpression.Literal -> {
                val strVal = expr.value.toString().removeSurrounding("'").removeSurrounding("\"")
                strVal.toDoubleOrNull() ?: strVal
            }
            is pql.model.PqlArithmeticExpression.Attribute -> {
                val repEvt = events.firstOrNull() ?: return null
                val tId = repEvt.get("traceId")?.takeIf { !it.isJsonNull }?.asString
                val lId = repEvt.get("logId")?.takeIf { !it.isJsonNull }?.asString

                val target = when (expr.scope) {
                    PqlScope.LOG -> if (lId != null) logDocsMap[lId] ?: repEvt else repEvt
                    PqlScope.TRACE -> if (tId != null) traceDocsMap[tId] ?: repEvt else repEvt
                    else -> repEvt
                }

                val ext = extractRobustValue(target, expr.scope, expr.attribute)
                ext as? Double ?: ext?.toString()?.toDoubleOrNull() ?: ext
            }
            is pql.model.PqlArithmeticExpression.FunctionCall -> {
                evaluateFunction(events, expr.function, traceDocsMap, logDocsMap)
            }
            is pql.model.PqlArithmeticExpression.Binary -> {
                val leftRaw = evaluateMath(events, expr.left, traceDocsMap, logDocsMap) ?: return null
                val rightRaw = evaluateMath(events, expr.right, traceDocsMap, logDocsMap) ?: return null

                if (expr.operator == pql.model.ArithmeticOperator.ADD && (leftRaw is String || rightRaw is String)) {
                    return leftRaw.toString() + rightRaw.toString()
                }

                if (expr.operator == pql.model.ArithmeticOperator.SUB && leftRaw is String && rightRaw is String) {
                    val dt1Str = leftRaw.toString().removeSurrounding("'").removeSurrounding("\"")
                    val dt2Str = rightRaw.toString().removeSurrounding("'").removeSurrounding("\"")

                    val dt1 = dateParsingCache.getOrPut(dt1Str) {
                        try {
                            val format = DateTimeFormatter.ISO_DATE_TIME
                            val ta = format.parseBest(dt1Str, Instant::from, OffsetDateTime::from)
                            if (ta is Instant) ta.toEpochMilli().toDouble() else (ta as OffsetDateTime).toInstant().toEpochMilli().toDouble()
                        } catch (e: Exception) { 0.0 }
                    }
                    val dt2 = dateParsingCache.getOrPut(dt2Str) {
                        try {
                            val format = DateTimeFormatter.ISO_DATE_TIME
                            val ta = format.parseBest(dt2Str, Instant::from, OffsetDateTime::from)
                            if (ta is Instant) ta.toEpochMilli().toDouble() else (ta as OffsetDateTime).toInstant().toEpochMilli().toDouble()
                        } catch (e: Exception) { 0.0 }
                    }

                    return (dt1 - dt2) / 86400000.0
                }

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

    @Suppress("UNCHECKED_CAST")
    private fun compareExtractedValues(a: Any?, b: Any?): Int {
        if (a == null && b == null) return 0
        if (a == null) return 1
        if (b == null) return -1

        if (a is Double && b is Double) return a.compareTo(b)
        if (a is Boolean && b is Boolean) return a.compareTo(b)

        return a.toString().compareTo(b.toString())
    }
}
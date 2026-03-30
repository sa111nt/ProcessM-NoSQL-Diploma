package pql.translator

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import pql.model.*

class PqlToCouchDbTranslator(
    private val fieldMapper: PqlFieldMapper = PqlFieldMapper()
) {

    private class ProjectionBypassException : RuntimeException()

    fun translateDelete(query: PqlDeleteQuery): JsonObject {
        val baseSelector = JsonObject().apply {
            if (query.scope != null) {
                addProperty("docType", query.scope.docType)
            }
        }

        val conditionSelector = if (query.conditions.isNotEmpty()) {
            val conditionJson = if (query.conditions.size == 1) {
                translateCondition(query.conditions[0], query.scope ?: PqlScope.EVENT)
            } else {
                JsonObject().apply {
                    add("\$and", JsonArray().apply {
                        query.conditions.forEach { condition ->
                            add(translateCondition(condition, query.scope ?: PqlScope.EVENT))
                        }
                    })
                }
            }
            mergeSelectors(baseSelector, conditionJson)
        } else {
            baseSelector
        }

        val json = JsonObject().apply {
            add("selector", conditionSelector)
            query.limit?.let { addProperty("limit", it) }
            query.offset?.let { addProperty("skip", it) }
        }

        return json
    }

    fun translate(query: PqlQuery): JsonObject {
        val baseSelector = JsonObject().apply {
            addProperty("docType", query.collection.docType)
        }

        val conditionSelector = if (query.conditions.isNotEmpty()) {
            val conditionJson = if (query.conditions.size == 1) {
                translateCondition(query.conditions[0], query.collection)
            } else {
                JsonObject().apply {
                    add("\$and", JsonArray().apply {
                        query.conditions.forEach { condition ->
                            add(translateCondition(condition, query.collection))
                        }
                    })
                }
            }
            mergeSelectors(baseSelector, conditionJson)
        } else {
            baseSelector
        }

        val json = JsonObject().apply {
            add("selector", conditionSelector)

            val fieldsArray = extractRequiredFields(query)
            if (fieldsArray != null && !fieldsArray.isEmpty) {
                add("fields", fieldsArray)
            }

            if (query.groupBy.isEmpty()) {
                query.limit?.let { addProperty("limit", it) }
                query.offset?.let { addProperty("skip", it) }
            }
        }

        if (query.groupBy.isEmpty() && query.orderBy.isNotEmpty()) {
            json.remove("limit")
            json.remove("skip")
            json.addProperty("limit", 100000)
        }

        return json
    }

    private fun extractRequiredFields(query: PqlQuery): JsonArray? {
        if (query.selectAll || query.projections.any { it.allAttributes || it.attribute == "*" }) return null

        val fields = mutableSetOf(
            "_id", "_rev", "docType", "logId", "traceId", "eventIndex",
            "activity", "timestamp", "xes_attributes._types",
            "t:concept:name", "l:concept:name",
            "identity:id", "t:identity:id", "l:identity:id", "originalTraceId"
        )
        val collection = query.collection

        try {
            query.conditions.forEach { extractFromCondition(it, collection, fields) }

            query.projections.forEach { proj ->
                safeAdd(proj.scope, proj.attribute, collection, fields)
                proj.function?.let { extractFromFunction(it, collection, fields) }
                proj.arithmeticExpression?.let { extractFromArithmetic(it, collection, fields) }
            }

            query.groupBy.forEach { safeAdd(it.scope, it.attribute, collection, fields) }

            query.orderBy.forEach { order ->
                safeAdd(order.scope, order.attribute, collection, fields)
                order.function?.let { extractFromFunction(it, collection, fields) }
                order.arithmeticExpression?.let { extractFromArithmetic(it, collection, fields) }
            }
        } catch (e: ProjectionBypassException) {
            return null
        }

        return JsonArray().apply { fields.forEach { add(it) } }
    }

    private fun safeAdd(scope: PqlScope?, attr: String?, collection: PqlScope, fields: MutableSet<String>) {
        if (attr == null) return
        if (attr == "*") throw ProjectionBypassException()
        val lowerAttr = attr.lowercase()
        if (lowerAttr.startsWith("c:") || lowerAttr.startsWith("e:c:") || lowerAttr.startsWith("[c:") || lowerAttr.startsWith("[e:c:") || lowerAttr.contains("classifier")) {
            throw ProjectionBypassException()
        }
        try {
            fields.add(fieldMapper.resolveForCondition(scope ?: collection, collection, attr).toDotPath())
        } catch (e: Exception) {
            throw ProjectionBypassException()
        }
    }

    private fun extractFromArithmetic(expr: PqlArithmeticExpression, collection: PqlScope, fields: MutableSet<String>) {
        when (expr) {
            is PqlArithmeticExpression.Attribute -> safeAdd(expr.scope, expr.attribute, collection, fields)
            is PqlArithmeticExpression.Binary -> {
                extractFromArithmetic(expr.left, collection, fields)
                extractFromArithmetic(expr.right, collection, fields)
            }
            is PqlArithmeticExpression.FunctionCall -> extractFromFunction(expr.function, collection, fields)
            is PqlArithmeticExpression.Literal -> {}
        }
    }

    private fun extractFromFunction(func: PqlFunction, collection: PqlScope, fields: MutableSet<String>) {
        when (func) {
            is PqlFunction.AggregationFunction -> safeAdd(func.scope, func.attribute, collection, fields)
            is PqlFunction.ScalarFunction1 -> extractFromArithmetic(func.expression, collection, fields)
            is PqlFunction.ScalarFunction0 -> {}
        }
    }

    private fun extractFromCondition(condition: PqlCondition, collection: PqlScope, fields: MutableSet<String>) {
        when (condition) {
            is PqlCondition.Simple -> safeAdd(condition.scope, condition.attribute, collection, fields)
            is PqlCondition.And -> condition.conditions.forEach { extractFromCondition(it, collection, fields) }
            is PqlCondition.Or -> condition.conditions.forEach { extractFromCondition(it, collection, fields) }
            is PqlCondition.Not -> extractFromCondition(condition.condition, collection, fields)
            else -> {}
        }
    }

    private fun translateCondition(condition: PqlCondition, collectionScope: PqlScope): JsonObject {
        return when (condition) {
            is PqlCondition.Simple -> translateSimpleCondition(condition, collectionScope)
            is PqlCondition.And -> {
                JsonObject().apply {
                    add("\$and", JsonArray().apply {
                        condition.conditions.forEach { cond ->
                            add(translateCondition(cond, collectionScope))
                        }
                    })
                }
            }
            is PqlCondition.Or -> {
                JsonObject().apply {
                    add("\$or", JsonArray().apply {
                        condition.conditions.forEach { cond ->
                            add(translateCondition(cond, collectionScope))
                        }
                    })
                }
            }
            is PqlCondition.Not -> {
                val inner = condition.condition
                if (inner is PqlCondition.Simple) {
                    when (inner.operator) {
                        PqlOperator.EQ -> translateSimpleCondition(PqlCondition.Simple(inner.scope, inner.attribute, PqlOperator.NEQ, inner.value), collectionScope)
                        PqlOperator.IN -> translateSimpleCondition(PqlCondition.Simple(inner.scope, inner.attribute, PqlOperator.NOT_IN, inner.value), collectionScope)
                        PqlOperator.LIKE, PqlOperator.MATCHES -> JsonObject().apply { add("\$nor", JsonArray().apply { add(translateSimpleCondition(inner, collectionScope)) }) }
                        else -> JsonObject().apply { add("\$not", translateCondition(inner, collectionScope)) }
                    }
                } else JsonObject().apply { add("\$not", translateCondition(inner, collectionScope)) }
            }
            is PqlCondition.AlwaysFalse -> JsonObject().apply { add("_id", JsonObject().apply { addProperty("\$eq", "__ALWAYS_FALSE__") }) }
        }
    }

    private fun translateSimpleCondition(condition: PqlCondition.Simple, collectionScope: PqlScope): JsonObject {
        val dotPath = fieldMapper.resolveForCondition(condition.scope, collectionScope, condition.attribute).toDotPath()
        val conditionObject = JsonObject()
        val strVal = condition.value as? String
        val numVal = strVal?.toDoubleOrNull()

        when (condition.operator) {
            PqlOperator.EQ -> if (numVal != null) conditionObject.add("\$in", JsonArray().apply { add(strVal); add(if (numVal % 1.0 == 0.0) numVal.toLong() else numVal) }) else conditionObject.add("\$eq", convertValue(condition.value))
            PqlOperator.NEQ -> if (numVal != null) conditionObject.add("\$nin", JsonArray().apply { add(strVal); add(if (numVal % 1.0 == 0.0) numVal.toLong() else numVal) }) else conditionObject.add("\$ne", convertValue(condition.value))
            PqlOperator.LT -> conditionObject.add("\$lt", convertValue(numVal ?: condition.value))
            PqlOperator.LE -> conditionObject.add("\$lte", convertValue(numVal ?: condition.value))
            PqlOperator.GT -> conditionObject.add("\$gt", convertValue(numVal ?: condition.value))
            PqlOperator.GE -> conditionObject.add("\$gte", convertValue(numVal ?: condition.value))
            PqlOperator.IN -> {
                val values = condition.value as? List<*> ?: error("IN requires list")
                conditionObject.add("\$in", JsonArray().apply { values.forEach { val vStr = it as? String; val vNum = vStr?.toDoubleOrNull(); add(convertValue(it)); if (vNum != null) add(convertValue(if (vNum % 1.0 == 0.0) vNum.toLong() else vNum)) } })
            }
            PqlOperator.NOT_IN -> {
                val values = condition.value as? List<*> ?: error("NOT IN requires list")
                conditionObject.add("\$nin", JsonArray().apply { values.forEach { val vStr = it as? String; val vNum = vStr?.toDoubleOrNull(); add(convertValue(it)); if (vNum != null) add(convertValue(if (vNum % 1.0 == 0.0) vNum.toLong() else vNum)) } })
            }
            PqlOperator.LIKE -> conditionObject.addProperty("\$regex", (condition.value as? String ?: error("LIKE requires string")).replace(".", "\\.").replace("%", ".*").replace("_", "."))
            PqlOperator.MATCHES -> conditionObject.addProperty("\$regex", condition.value as? String ?: error("MATCHES requires string"))
            PqlOperator.IS_NULL -> conditionObject.addProperty("\$exists", false)
            PqlOperator.IS_NOT_NULL -> conditionObject.addProperty("\$exists", true)
        }
        return JsonObject().apply { add(dotPath, conditionObject) }
    }

    private fun convertValue(value: Any?): com.google.gson.JsonElement {
        return when (value) {
            null -> com.google.gson.JsonNull.INSTANCE
            is String -> when(value) { "true" -> com.google.gson.JsonPrimitive(true); "false" -> com.google.gson.JsonPrimitive(false); "null" -> com.google.gson.JsonNull.INSTANCE; else -> com.google.gson.JsonPrimitive(value) }
            is Number -> com.google.gson.JsonPrimitive(value)
            is Boolean -> com.google.gson.JsonPrimitive(value)
            is List<*> -> JsonArray().apply { value.forEach { add(convertValue(it)) } }
            else -> com.google.gson.JsonPrimitive(value.toString())
        }
    }

    private fun mergeSelectors(base: JsonObject, additional: JsonObject): JsonObject {
        val merged = JsonObject()
        base.entrySet().forEach { merged.add(it.key, it.value) }
        additional.entrySet().forEach { (key, value) ->
            if (merged.has(key)) {
                val existing = merged.get(key)
                if (existing.isJsonObject && value.isJsonObject) {
                    val existingObj = existing.asJsonObject
                    value.asJsonObject.entrySet().forEach { existingObj.add(it.key, it.value) }
                } else {
                    val andArray = JsonArray().apply { add(existing); add(value) }
                    merged.remove(key); merged.add("\$and", andArray)
                }
            } else merged.add(key, value)
        }
        return merged
    }

    fun resolveField(scope: PqlScope, attribute: String): FieldPath = fieldMapper.resolve(scope, attribute)
}
package pql.translator

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import pql.model.*

class PqlToCouchDbTranslator(
    private val fieldMapper: PqlFieldMapper = PqlFieldMapper()
) {

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
                        PqlOperator.EQ -> {
                            translateSimpleCondition(
                                PqlCondition.Simple(inner.scope, inner.attribute, PqlOperator.NEQ, inner.value),
                                collectionScope
                            )
                        }
                        PqlOperator.LIKE -> {
                            val dotPath = fieldMapper.resolve(inner.scope, inner.attribute).toDotPath()
                            val pattern = inner.value as? String ?: error("LIKE operator requires a string pattern")
                            val regexPattern = pattern
                                .replace(".", "\\.")
                                .replace("%", ".*")
                                .replace("_", ".")

                            val likeCondition = JsonObject()
                            likeCondition.add(dotPath, JsonObject().apply { addProperty("\$regex", regexPattern) })

                            JsonObject().apply {
                                add("\$nor", JsonArray().apply {
                                    add(likeCondition)
                                })
                            }
                        }
                        else -> {
                            JsonObject().apply {
                                add("\$not", translateCondition(inner, collectionScope))
                            }
                        }
                    }
                } else {
                    JsonObject().apply {
                        add("\$not", translateCondition(inner, collectionScope))
                    }
                }
            }
            is PqlCondition.AlwaysFalse -> {
                JsonObject().apply {
                    add("_id", JsonObject().apply { addProperty("\$eq", "__ALWAYS_FALSE__") })
                }
            }
        }
    }

    private fun translateSimpleCondition(
        condition: PqlCondition.Simple,
        collectionScope: PqlScope
    ): JsonObject {
        var dotPath = fieldMapper.resolve(condition.scope, condition.attribute).toDotPath()

        // 🚨 NAPRAWA BŁĘDU MAPOWANIA KLUCZY OBCYCH 🚨
        // Jeśli szukamy w Eventach, a ktoś prosi o l:id, musimy szukać po "logId", a nie po "_id" eventu!
        val attrLower = condition.attribute.lowercase()
        if (condition.scope != collectionScope && (attrLower == "id" || attrLower == "_id")) {
            if (condition.scope == PqlScope.LOG) {
                dotPath = "logId"
            } else if (condition.scope == PqlScope.TRACE) {
                dotPath = "traceId"
            }
        }

        val conditionObject = JsonObject()

        when (condition.operator) {
            PqlOperator.EQ -> conditionObject.add("\$eq", convertValue(condition.value))
            PqlOperator.NEQ -> conditionObject.add("\$ne", convertValue(condition.value))
            PqlOperator.LT -> conditionObject.add("\$lt", convertValue(condition.value))
            PqlOperator.LE -> conditionObject.add("\$lte", convertValue(condition.value))
            PqlOperator.GT -> conditionObject.add("\$gt", convertValue(condition.value))
            PqlOperator.GE -> conditionObject.add("\$gte", convertValue(condition.value))
            PqlOperator.IN -> {
                val values = condition.value as? List<*> ?: error("IN operator requires a list of values")
                conditionObject.add("\$in", JsonArray().apply {
                    values.forEach { add(convertValue(it)) }
                })
            }
            PqlOperator.NOT_IN -> {
                val values = condition.value as? List<*> ?: error("NOT IN operator requires a list of values")
                conditionObject.add("\$nin", JsonArray().apply {
                    values.forEach { add(convertValue(it)) }
                })
            }
            PqlOperator.LIKE -> {
                val pattern = condition.value as? String ?: error("LIKE operator requires a string pattern")
                val regexPattern = pattern.replace(".", "\\.").replace("%", ".*").replace("_", ".")
                conditionObject.addProperty("\$regex", regexPattern)
            }
            PqlOperator.MATCHES -> {
                val pattern = condition.value as? String ?: error("MATCHES operator requires a string pattern")
                conditionObject.addProperty("\$regex", pattern)
            }
            PqlOperator.IS_NULL -> conditionObject.addProperty("\$exists", false)
            PqlOperator.IS_NOT_NULL -> conditionObject.addProperty("\$exists", true)
        }

        val result = JsonObject()
        result.add(dotPath, conditionObject)
        return result
    }

    private fun convertValue(value: Any?): com.google.gson.JsonElement {
        return when (value) {
            null -> com.google.gson.JsonNull.INSTANCE
            is String -> {
                when {
                    value == "true" -> com.google.gson.JsonPrimitive(true)
                    value == "false" -> com.google.gson.JsonPrimitive(false)
                    value == "null" -> com.google.gson.JsonNull.INSTANCE
                    else -> com.google.gson.JsonPrimitive(value)
                }
            }
            is Number -> com.google.gson.JsonPrimitive(value.toString())
            is Boolean -> com.google.gson.JsonPrimitive(value)
            is List<*> -> {
                JsonArray().apply {
                    value.forEach { item ->
                        add(convertValue(item))
                    }
                }
            }
            else -> com.google.gson.JsonPrimitive(value.toString())
        }
    }

    private fun mergeSelectors(base: JsonObject, additional: JsonObject): JsonObject {
        val merged = JsonObject()

        base.entrySet().forEach { (key, value) ->
            merged.add(key, value)
        }

        additional.entrySet().forEach { (key, value) ->
            if (merged.has(key)) {
                val existing = merged.get(key)
                if (existing.isJsonObject && value.isJsonObject) {
                    val existingObj = existing.asJsonObject
                    val newObj = value.asJsonObject
                    existingObj.entrySet().forEach { (k, v) ->
                        newObj.add(k, v)
                    }
                    merged.add(key, newObj)
                } else {
                    val andArray = JsonArray()
                    andArray.add(existing)
                    andArray.add(value)
                    merged.remove(key)
                    merged.add("\$and", andArray)
                }
            } else {
                merged.add(key, value)
            }
        }

        return merged
    }

    fun resolveField(scope: PqlScope, attribute: String): FieldPath =
        fieldMapper.resolve(scope, attribute)
}
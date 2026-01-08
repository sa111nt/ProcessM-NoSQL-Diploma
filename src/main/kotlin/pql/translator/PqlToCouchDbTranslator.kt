package pql.translator

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import pql.model.*

class PqlToCouchDbTranslator(
    private val fieldMapper: PqlFieldMapper = PqlFieldMapper()
) {

    fun translateDelete(query: PqlDeleteQuery): JsonObject {
        // Build base selector with docType filter if scope is specified
        val baseSelector = JsonObject().apply {
            if (query.scope != null) {
                addProperty("docType", query.scope.docType)
            }
        }
        
        // Build condition selector from WHERE clause
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

        query.orderBy?.let { order ->
            val sortField = fieldMapper.resolve(order.scope, order.attribute).toDotPath()
            val sortArray = JsonArray()
            val sortObject = JsonObject()
            sortObject.addProperty(sortField, order.direction.name.lowercase())
            sortArray.add(sortObject)
            json.add("sort", sortArray)
        }

        return json
    }

    fun translate(query: PqlQuery): JsonObject {
        // Build base selector with docType filter
        val baseSelector = JsonObject().apply {
            addProperty("docType", query.collection.docType)
        }
        
        // Build condition selector from WHERE clause
        val conditionSelector = if (query.conditions.isNotEmpty()) {
            // Translate the condition(s) - typically there's one root condition that may contain AND/OR
            val conditionJson = if (query.conditions.size == 1) {
                translateCondition(query.conditions[0], query.collection)
            } else {
                // Multiple top-level conditions should be combined with AND
                JsonObject().apply {
                    add("\$and", JsonArray().apply {
                        query.conditions.forEach { condition ->
                            add(translateCondition(condition, query.collection))
                        }
                    })
                }
            }
            
            // Merge base selector with condition selector
            mergeSelectors(baseSelector, conditionJson)
        } else {
            baseSelector
        }

        val json = JsonObject().apply {
            add("selector", conditionSelector)
            // For GROUP BY queries, don't apply LIMIT/OFFSET at CouchDB level
            // We'll apply them after grouping in PqlInterpreter
            if (query.groupBy.isEmpty()) {
                query.limit?.let { addProperty("limit", it) }
                query.offset?.let { addProperty("skip", it) }
            }
        }

        // ORDER BY is only applied for non-GROUP BY queries
        // For GROUP BY queries, ORDER BY is not supported
        if (query.groupBy.isEmpty()) {
            query.orderBy?.let { order ->
                require(order.scope == query.collection) {
                    "This interpreter currently supports ordering within the ${query.collection.label} scope."
                }
                val sortField = fieldMapper.resolve(order.scope, order.attribute).toDotPath()
                val sortArray = JsonArray()
                val sortObject = JsonObject()
                sortObject.addProperty(sortField, order.direction.name.lowercase())
                sortArray.add(sortObject)
                json.add("sort", sortArray)
            }
        }

        return json
    }
    
    /**
     * Translate a PqlCondition to CouchDB Mango query format
     */
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
                // Optimize: if NOT wraps a simple condition, convert to opposite operator
                val inner = condition.condition
                if (inner is PqlCondition.Simple) {
                    when (inner.operator) {
                        PqlOperator.EQ -> {
                            // Convert NOT (attr = value) to (attr != value)
                            translateSimpleCondition(
                                PqlCondition.Simple(inner.scope, inner.attribute, PqlOperator.NEQ, inner.value),
                                collectionScope
                            )
                        }
                        PqlOperator.LIKE -> {
                            // Convert NOT (attr LIKE pattern) to (attr NOT LIKE pattern)
                            // CouchDB doesn't support $not with $regex directly, so we use $nor
                            val fieldPath = fieldMapper.resolve(inner.scope, inner.attribute)
                            val pattern = inner.value as? String ?: error("LIKE operator requires a string pattern")
                            val regexPattern = pattern
                                .replace(".", "\\.")
                                .replace("%", ".*")
                                .replace("_", ".")
                            
                            // Build the LIKE condition
                            val likeCondition = JsonObject()
                            val likeConditionObject = JsonObject().apply {
                                addProperty("\$regex", regexPattern)
                            }
                            addConditionToPath(likeCondition, fieldPath, likeConditionObject)
                            
                            // Use $nor to negate the LIKE condition
                            JsonObject().apply {
                                add("\$nor", JsonArray().apply {
                                    add(likeCondition)
                                })
                            }
                        }
                        else -> {
                            // For other simple conditions, use $not
                            JsonObject().apply {
                                add("\$not", translateCondition(inner, collectionScope))
                            }
                        }
                    }
                } else {
                    // For complex NOT conditions, use $not
                    JsonObject().apply {
                        add("\$not", translateCondition(inner, collectionScope))
                    }
                }
            }
        }
    }
    
    private fun translateSimpleCondition(
        condition: PqlCondition.Simple,
        collectionScope: PqlScope
    ): JsonObject {
        require(condition.scope == collectionScope) {
            "This interpreter currently supports conditions only on the ${collectionScope.label} scope."
        }
        
        val fieldPath = fieldMapper.resolve(condition.scope, condition.attribute)
        val conditionObject = JsonObject()
        
        when (condition.operator) {
            PqlOperator.EQ -> {
                conditionObject.add("\$eq", convertValue(condition.value))
            }
            PqlOperator.NEQ -> {
                conditionObject.add("\$ne", convertValue(condition.value))
            }
            PqlOperator.LT -> {
                conditionObject.add("\$lt", convertValue(condition.value))
            }
            PqlOperator.LE -> {
                conditionObject.add("\$lte", convertValue(condition.value))
            }
            PqlOperator.GT -> {
                conditionObject.add("\$gt", convertValue(condition.value))
            }
            PqlOperator.GE -> {
                conditionObject.add("\$gte", convertValue(condition.value))
            }
            PqlOperator.IN -> {
                val values = condition.value as? List<*>
                    ?: error("IN operator requires a list of values")
                conditionObject.add("\$in", JsonArray().apply {
                    values.forEach { value ->
                        add(convertValue(value))
                    }
                })
            }
            PqlOperator.NOT_IN -> {
                val values = condition.value as? List<*>
                    ?: error("NOT IN operator requires a list of values")
                conditionObject.add("\$nin", JsonArray().apply {
                    values.forEach { value ->
                        add(convertValue(value))
                    }
                })
            }
            PqlOperator.LIKE -> {
                val pattern = condition.value as? String
                    ?: error("LIKE operator requires a string pattern")
                // Convert SQL LIKE pattern to regex
                val regexPattern = pattern
                    .replace(".", "\\.")
                    .replace("%", ".*")
                    .replace("_", ".")
                conditionObject.addProperty("\$regex", regexPattern)
            }
            PqlOperator.MATCHES -> {
                val pattern = condition.value as? String
                    ?: error("MATCHES operator requires a string pattern")
                conditionObject.addProperty("\$regex", pattern)
            }
            PqlOperator.IS_NULL -> {
                conditionObject.addProperty("\$exists", false)
            }
            PqlOperator.IS_NOT_NULL -> {
                conditionObject.addProperty("\$exists", true)
            }
        }
        
        // Build nested object structure for field path
        val result = JsonObject()
        addConditionToPath(result, fieldPath, conditionObject)
        return result
    }
    
    private fun convertValue(value: Any?): com.google.gson.JsonElement {
        return when (value) {
            null -> com.google.gson.JsonNull.INSTANCE
            is String -> {
                // Try to parse as number or boolean
                when {
                    value == "true" -> com.google.gson.JsonPrimitive(true)
                    value == "false" -> com.google.gson.JsonPrimitive(false)
                    value == "null" -> com.google.gson.JsonNull.INSTANCE
                    value.toDoubleOrNull() != null -> com.google.gson.JsonPrimitive(value.toDouble())
                    else -> com.google.gson.JsonPrimitive(value)
                }
            }
            is Number -> com.google.gson.JsonPrimitive(value)
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
        
        // Copy base selector
        base.entrySet().forEach { (key, value) ->
            merged.add(key, value)
        }
        
        // Merge additional selector
        additional.entrySet().forEach { (key, value) ->
            if (merged.has(key)) {
                // If both have the same key, we need to combine them with $and
                val existing = merged.get(key)
                if (existing.isJsonObject && value.isJsonObject) {
                    // Merge objects
                    val existingObj = existing.asJsonObject
                    val newObj = value.asJsonObject
                    existingObj.entrySet().forEach { (k, v) ->
                        newObj.add(k, v)
                    }
                    merged.add(key, newObj)
                } else {
                    // Use $and to combine
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

    private fun addConditionToPath(target: JsonObject, fieldPath: FieldPath, conditionObject: JsonObject) {
        var current = target
        val segments = fieldPath.segments
        for (i in 0 until segments.lastIndex) {
            val segment = segments[i]
            val next = if (current.has(segment)) {
                current.get(segment)?.asJsonObject
                    ?: error("Selector path conflict at $segment")
            } else {
                JsonObject().also { current.add(segment, it) }
            }
            current = next
        }
        val leaf = segments.last()
        val existing = current.get(leaf)
        if (existing == null) {
            current.add(leaf, conditionObject)
        } else {
            val existingObject = existing.asJsonObject
            conditionObject.entrySet().forEach { (key, value) ->
                existingObject.add(key, value)
            }
        }
    }
}


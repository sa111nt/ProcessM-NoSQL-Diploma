package pql

import com.google.gson.JsonArray
import com.google.gson.JsonObject

class PqlToCouchDbTranslator(
    private val fieldMapper: PqlFieldMapper = PqlFieldMapper()
) {

    fun translate(query: PqlQuery): JsonObject {
        val selector = JsonObject().apply {
            addCondition(FieldPath(listOf("docType")), JsonObject().apply {
                addProperty("\$eq", query.collection.docType)
            })
        }

        query.conditions.forEach { condition ->
            require(condition.scope == query.collection) {
                "This interpreter currently supports conditions only on the ${query.collection.label} scope." //zmodyfikowac w razie potrzeb
            }
            val fieldPath = fieldMapper.resolve(condition.scope, condition.attribute)
            val conditionObject = JsonObject().apply {
                when (condition.operator) {
                    PqlOperator.EQ -> addProperty("\$eq", condition.value)
                    PqlOperator.NEQ -> addProperty("\$ne", condition.value)
                }
            }
            selector.addCondition(fieldPath, conditionObject)
        }

        val json = JsonObject().apply {
            add("selector", selector)
            query.limit?.let { addProperty("limit", it) }
        }

        query.orderBy?.let { order ->
            require(order.scope == query.collection) {
                "This interpreter currently supports ordering within the ${query.collection.label} scope." //zmodyfikowac w razie potrzeb
            }
            val sortField = fieldMapper.resolve(order.scope, order.attribute).toDotPath()
            val sortArray = JsonArray()
            val sortObject = JsonObject()
            sortObject.addProperty(sortField, order.direction.name.lowercase())
            sortArray.add(sortObject)
            json.add("sort", sortArray)
        }

        return json
    }

    fun resolveField(scope: PqlScope, attribute: String): FieldPath =
        fieldMapper.resolve(scope, attribute)

    private fun JsonObject.addCondition(fieldPath: FieldPath, conditionObject: JsonObject) {
        var current = this
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


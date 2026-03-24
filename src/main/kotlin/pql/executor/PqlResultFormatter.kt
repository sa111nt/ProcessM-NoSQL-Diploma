package pql.executor

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import pql.model.PqlProjection
import pql.model.PqlQuery
import pql.model.PqlScope
import pql.translator.PqlFieldMapper

class PqlResultFormatter(private val fieldMapper: PqlFieldMapper) {

    fun format(rawDocs: JsonArray, query: PqlQuery): JsonArray {
        val result = JsonArray()
        for (element in rawDocs) {
            if (!element.isJsonObject) continue
            val doc = element.asJsonObject
            val formatted = formatDocument(doc, query)
            result.add(formatted)
        }
        return result
    }

    private fun formatDocument(doc: JsonObject, query: PqlQuery): JsonObject {
        val result = JsonObject()

        doc.get("logId")?.let { result.add("logId", it) }
        doc.get("traceId")?.let { result.add("traceId", it) }
        doc.get("_id")?.let { result.add("_id", it) }

        if (query.selectAll || query.projections.isEmpty()) {
            val allFields = getAllFields(doc, query.collection)
            for ((pqlName, value) in allFields) {
                result.add(pqlName, value)
            }
            return result
        }

        for (projection in query.projections) {
            val alias = projection.alias ?: projection.raw

            if (projection.allAttributes) {
                val scope = projection.scope ?: PqlScope.EVENT
                val fields = getAllFields(doc, scope)
                for ((k, v) in fields) {
                    if (k.startsWith(scope.aliases.first() + ":")) {
                        result.add(k, v)
                    }
                }
            } else if (projection.attribute != null) {
                val attrValue = extractField(doc, projection.scope ?: PqlScope.EVENT, projection.attribute)
                if (attrValue != null) result.add(alias, attrValue)
            } else if (projection.function != null || projection.arithmeticExpression != null) {
                if (doc.has(projection.raw)) result.add(alias, doc.get(projection.raw))
            }
        }

        return result
    }

    private fun extractField(doc: JsonObject, scope: PqlScope, attr: String): JsonElement? {
        val cleanAttr = fieldMapper.getStandardName(attr).removePrefix("^^").removePrefix("^")

        val isClassifier = cleanAttr.startsWith("c:") || cleanAttr.startsWith("classifier:") || cleanAttr.startsWith("e:c:") || cleanAttr.startsWith("t:c:") || cleanAttr.startsWith("l:c:")
        if (isClassifier) {
            val baseAttr = cleanAttr.removePrefix("e:c:").removePrefix("c:").removePrefix("classifier:")
            val mappedFields = fieldMapper.getImplicitClassifierMapping(baseAttr)
            val vals = mappedFields.mapNotNull { part ->
                PqlQueryExecutor.extractRobustValue(doc, scope, part)?.toString()?.takeIf { it.isNotBlank() && it != "null" }
            }
            if (vals.isNotEmpty()) return com.google.gson.JsonPrimitive(vals.joinToString("+").intern())
            return null
        }

        val rawVal = PqlQueryExecutor.extractRobustValue(doc, scope, cleanAttr) ?: return null

        return when (rawVal) {
            is String -> com.google.gson.JsonPrimitive(rawVal)
            is Number -> com.google.gson.JsonPrimitive(rawVal)
            is Boolean -> com.google.gson.JsonPrimitive(rawVal)
            else -> com.google.gson.JsonPrimitive(rawVal.toString())
        }
    }

    private fun getAllFields(doc: JsonObject, scope: PqlScope): Map<String, JsonElement> {
        val fields = mutableMapOf<String, JsonElement>()

        doc.get("activity")?.let { fields["e:concept:name"] = it }
        doc.get("timestamp")?.let { fields["e:time:timestamp"] = it }
        doc.get("identity:id")?.let { fields["e:identity:id"] = it }

        doc.getAsJsonObject("xes_attributes")?.entrySet()?.forEach { fields["e:${it.key}"] = it.value }

        doc.entrySet().forEach { (k, v) ->
            if (k.startsWith("l:") || k.startsWith("t:") || k.startsWith("e:")) {
                fields[k] = v
            }
        }

        return fields
    }
}
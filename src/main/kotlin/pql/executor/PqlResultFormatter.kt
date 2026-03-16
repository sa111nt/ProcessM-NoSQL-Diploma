package pql.executor

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import pql.model.PqlProjection
import pql.model.PqlQuery
import pql.model.PqlScope
import pql.translator.PqlFieldMapper

/**
 * Formatuje surowe dokumenty z CouchDB na wynik zapytania PQL.
 * Zachowuje oryginalne przestrzenie nazw (e:, t:, l:), aby uniknąć kolizji
 * kluczy przy zapytaniach Cross-Scope (np. t:concept:name vs e:concept:name).
 */
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

        if (query.selectAll) {
            val allFields = getAllFields(doc, query.collection)
            for ((pqlName, value) in allFields) {
                result.add(pqlName, value)
            }
            return result
        }

        for (projection in query.projections) {
            // Wartości wyliczone w pamięci (arytmetyka/agregacje)
            if (projection.arithmeticExpression != null || projection.function != null) {
                val rawKey = projection.raw
                val cleanKey = projection.raw.replace(" ", "")

                val computedValue = doc.get(rawKey) ?: doc.get(cleanKey)
                if (computedValue != null) {
                    val aliasName = projection.alias ?: rawKey
                    result.add(aliasName, computedValue)
                }
                continue
            }

            val scope = projection.scope ?: query.collection
            val attribute = projection.attribute ?: continue

            if (projection.allAttributes) {
                val allFields = getAllFields(doc, scope)
                for ((pqlName, value) in allFields) {
                    result.add(pqlName, value)
                }
                continue
            }

            val value = extractValue(doc, scope, attribute)
            val pqlName = projection.alias ?: buildPqlFieldName(scope, attribute)

            if (value != null) {
                result.add(pqlName, value)
            } else if (scope == PqlScope.TRACE && (attribute == "name" || attribute == "concept:name")) {
                doc.get("traceId")?.let { result.add(pqlName, it) }
            }
        }

        // Sprawdzamy, czy w zapytaniu były wyłącznie agregacje/działania.
        val hasAggregationsOnly = query.projections.isNotEmpty() && query.projections.all { it.function != null || it.arithmeticExpression != null }

        // Poprawka dla Process Mining (Zapobieganie pustym reprezentacjom {})
        if (result.entrySet().isEmpty() && !hasAggregationsOnly) {
            extractValue(doc, PqlScope.EVENT, "concept:name")?.let { result.add("e:concept:name", it) }
            extractValue(doc, PqlScope.EVENT, "time:timestamp")?.let { result.add("e:time:timestamp", it) }
        }

        // BEZPIECZNIK NOSQL (Przeniesiony z testów do Core'a):
        // Jeśli zapytanie powinno zwrócić dane powiązane ze śladem, ale atrybut t:name zniknął
        // (np. przez dynamikę grupowania MangoDB), silnik odzyska go z surowego dokumentu
        // i umieści w ładunku dla końcowego użytkownika.
        if (!result.has("t:name") && !hasAggregationsOnly) {
            val traceName = doc.get("traceId")?.asString
                ?: doc.get("t:concept:name")?.asString
                ?: Regex("log_[a-f0-9\\-]+_\\-?\\d+").find(doc.toString())?.value

            if (traceName != null) {
                result.addProperty("t:name", traceName)
            }
        }

        return result
    }

    fun extractValue(doc: JsonObject, scope: PqlScope, attribute: String): JsonElement? {
        val fieldPath = fieldMapper.resolve(scope, attribute)
        val segments = fieldPath.segments

        var current: JsonElement = doc
        for (segment in segments) {
            if (current.isJsonObject && current.asJsonObject.has(segment)) {
                current = current.asJsonObject.get(segment)
            } else {
                return null
            }
        }

        return current
    }

    private fun buildPqlFieldName(scope: PqlScope, attribute: String): String {
        val prefix = when (scope) {
            PqlScope.LOG -> "l"
            PqlScope.TRACE -> "t"
            PqlScope.EVENT -> "e"
        }
        return "$prefix:$attribute"
    }

    private fun getAllFields(doc: JsonObject, scope: PqlScope): Map<String, JsonElement> {
        val fields = mutableMapOf<String, JsonElement>()

        when (scope) {
            PqlScope.EVENT -> {
                doc.get("activity")?.let { fields["e:concept:name"] = it }
                doc.get("timestamp")?.let { fields["e:time:timestamp"] = it }

                doc.getAsJsonObject("xes_attributes")?.entrySet()?.forEach { (key, value) ->
                    fields["e:$key"] = value
                }
            }
            PqlScope.TRACE -> {
                doc.get("originalTraceId")?.let { fields["t:concept:name"] = it }

                doc.getAsJsonObject("xes_attributes")?.entrySet()?.forEach { (key, value) ->
                    fields["t:$key"] = value
                }
            }
            PqlScope.LOG -> {
                doc.get("source")?.let { fields["l:source"] = it }

                doc.getAsJsonObject("log_attributes")?.entrySet()?.forEach { (key, value) ->
                    fields["l:$key"] = value
                }
            }
        }

        return fields
    }
}
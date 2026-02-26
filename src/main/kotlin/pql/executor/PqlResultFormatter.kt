package pql.executor

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import pql.model.PqlProjection
import pql.model.PqlQuery
import pql.model.PqlScope
import pql.translator.PqlFieldMapper

/**
 * Formatuje surowe dokumenty z CouchDB na czysty wynik zapytania PQL.
 *
 * Przykład:
 *   Zapytanie: SELECT e:concept:name, e:time:timestamp
 *   Surowy dokument: {"_id":"...", "activity":"register", "timestamp":"...", "xes_attributes":{...}}
 *   Po formatowaniu: {"e:concept:name":"register", "e:time:timestamp":"..."}
 */
class PqlResultFormatter(private val fieldMapper: PqlFieldMapper) {

    /**
     * Główna metoda — formatuje tablicę surowych dokumentów.
     * @param rawDocs Surowe dokumenty z CouchDB (wynik findDocs)
     * @param query Sparsowane zapytanie PQL (zawiera projections, selectAll itp.)
     * @return JsonArray z sformatowanymi dokumentami (tylko potrzebne pola)
     */
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

    /**
     * Formatuje jeden dokument — wybiera tylko żądane pola.
     */
    private fun formatDocument(doc: JsonObject, query: PqlQuery): JsonObject {
        val result = JsonObject()

        // Jeśli SELECT * — zwracamy wszystkie pola z nazwami PQL
        if (query.selectAll) {
            val allFields = getAllFields(doc, query.collection)
            for ((pqlName, value) in allFields) {
                result.add(pqlName, value)
            }
            return result
        }

        // W przeciwnym razie — tylko żądane projekcje
        for (projection in query.projections) {
            val scope = projection.scope ?: query.collection
            val attribute = projection.attribute ?: continue

            // Jeśli projection to e:* lub t:* (wszystkie atrybuty danego scope'u)
            if (projection.allAttributes) {
                val allFields = getAllFields(doc, scope)
                for ((pqlName, value) in allFields) {
                    result.add(pqlName, value)
                }
                continue
            }

            // Zwykła projekcja: e:concept:name → wyciągamy wartość
            val value = extractValue(doc, scope, attribute)
            val pqlName = projection.alias ?: buildPqlFieldName(scope, attribute)
            if (value != null) {
                result.add(pqlName, value)
            }
        }

        return result
    }

    /**
     * Wyciąga wartość pola z dokumentu CouchDB.
     *
     * Używa PqlFieldMapper do mapowania:
     *   e:concept:name → resolve(EVENT, "concept:name") → FieldPath(["activity"]) → doc.get("activity")
     *   e:cost:total → resolve(EVENT, "cost:total") → FieldPath(["xes_attributes", "cost:total"])
     */
    fun extractValue(doc: JsonObject, scope: PqlScope, attribute: String): JsonElement? {
        val fieldPath = fieldMapper.resolve(scope, attribute)
        val segments = fieldPath.segments

        // Przechodzimy po ścieżce: doc → "xes_attributes" → "cost:total"
        var current: JsonElement = doc
        for (segment in segments) {
            if (current.isJsonObject && current.asJsonObject.has(segment)) {
                current = current.asJsonObject.get(segment)
            } else {
                return null // Pole nie znalezione
            }
        }

        return current
    }

    /**
     * Buduje nazwę pola w formacie PQL: EVENT + "concept:name" → "e:concept:name"
     */
    private fun buildPqlFieldName(scope: PqlScope, attribute: String): String {
        val prefix = when (scope) {
            PqlScope.LOG -> "l"
            PqlScope.TRACE -> "t"
            PqlScope.EVENT -> "e"
        }
        return "$prefix:$attribute"
    }

    /**
     * Dla SELECT * — zwraca wszystkie pola dokumentu z nazwami PQL.
     * Pola standardowe (activity, timestamp) + wszystko z xes_attributes.
     */
    private fun getAllFields(doc: JsonObject, scope: PqlScope): Map<String, JsonElement> {
        val fields = mutableMapOf<String, JsonElement>()

        when (scope) {
            PqlScope.EVENT -> {
                // Pola standardowe
                doc.get("activity")?.let { fields["e:concept:name"] = it }
                doc.get("timestamp")?.let { fields["e:time:timestamp"] = it }

                // Wszystkie pola z xes_attributes
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

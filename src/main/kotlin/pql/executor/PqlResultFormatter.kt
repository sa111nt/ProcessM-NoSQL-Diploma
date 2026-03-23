package pql.executor

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import pql.model.PqlProjection
import pql.model.PqlQuery
import pql.model.PqlScope
import pql.model.PqlStandardAttributes
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

        // --- CZYSTA ARCHITEKTURA: Podejście Dwóch Przejść (Two-Pass Approach) ---
        // Gwarantuje zachowanie kolejności kluczy JSON względem zapytania SELECT,
        // ale skutecznie odrzuca zduplikowane klasyfikatory w locie.

        val activeBaseAttributes = mutableSetOf<String>()

        for (proj in query.projections) {
            if (proj.arithmeticExpression != null || proj.function != null) continue
            val scope = proj.scope ?: query.collection
            val attr = proj.attribute ?: continue
            val resolvedAttr = PqlStandardAttributes.resolve(scope, attr).removePrefix("^^").removePrefix("^").removePrefix("[").removeSuffix("]")
            val isClassifier = resolvedAttr.startsWith("classifier:") || resolvedAttr.startsWith("c:") || resolvedAttr.startsWith("e:c:") || resolvedAttr.startsWith("t:c:") || resolvedAttr.startsWith("l:c:")

            if (!isClassifier) {
                activeBaseAttributes.add("${scope.name}_${fieldMapper.getStandardName(resolvedAttr)}")
            }
        }

        val processedBases = mutableSetOf<String>()

        for (projection in query.projections) {
            if (projection.arithmeticExpression != null || projection.function != null) {
                val rawKey = projection.raw
                val cleanKey = projection.raw.replace(" ", "")

                val computedValue = doc.get(rawKey) ?: doc.get(cleanKey)
                if (computedValue != null) {
                    val aliasName = projection.alias ?: rawKey
                    if (aliasName.lowercase().contains("count") && computedValue.isJsonPrimitive && computedValue.asJsonPrimitive.isNumber) {
                        result.addProperty(aliasName, computedValue.asDouble.toLong())
                    } else {
                        result.add(aliasName, computedValue)
                    }
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

            val resolvedAttr = PqlStandardAttributes.resolve(scope, attribute)
            val cleanAttrOriginal = resolvedAttr.removePrefix("^^").removePrefix("^").removePrefix("[").removeSuffix("]")

            val isClassifier = cleanAttrOriginal.startsWith("classifier:") || cleanAttrOriginal.startsWith("c:") || cleanAttrOriginal.startsWith("e:c:") || cleanAttrOriginal.startsWith("t:c:") || cleanAttrOriginal.startsWith("l:c:")

            val baseAttributes = if (isClassifier) {
                val base = cleanAttrOriginal.substringAfterLast(":")
                fieldMapper.getImplicitClassifierMapping(base)
            } else {
                listOf(cleanAttrOriginal)
            }

            var isDuplicate = false
            if (isClassifier) {
                isDuplicate = baseAttributes.any {
                    val mapped = "${scope.name}_${fieldMapper.getStandardName(it)}"
                    activeBaseAttributes.contains(mapped) || processedBases.contains(mapped)
                }
            } else {
                isDuplicate = baseAttributes.any {
                    val mapped = "${scope.name}_${fieldMapper.getStandardName(it)}"
                    processedBases.contains(mapped)
                }
            }

            val pqlName = projection.alias ?: projection.raw

            // CZYSTA DEDUPLIKACJA BEZ HACKÓW
            if (isDuplicate) {
                continue
            }

            baseAttributes.forEach { processedBases.add("${scope.name}_${fieldMapper.getStandardName(it)}") }

            val value = extractValue(doc, scope, attribute)

            if (value != null) {
                addValueToResult(result, pqlName, value)
            } else if (scope == PqlScope.TRACE && (attribute == "name" || attribute == "concept:name")) {
                doc.get("traceId")?.let { result.addProperty(pqlName, it.asString.substringAfterLast("_")) }
            }
        }
        return result
    }

    private fun addValueToResult(result: JsonObject, pqlName: String, value: JsonElement) {
        if (pqlName.lowercase().contains("count") && value.isJsonPrimitive && value.asJsonPrimitive.isNumber) {
            result.addProperty(pqlName, value.asDouble.toLong())
        } else if (value.isJsonPrimitive) {
            val prim = value.asJsonPrimitive
            if (prim.isNumber) {
                val num = prim.asNumber
                if (num.toDouble() % 1.0 == 0.0) {
                    result.addProperty(pqlName, num.toLong())
                } else {
                    result.addProperty(pqlName, num.toDouble())
                }
            }
            else if (prim.isBoolean) result.addProperty(pqlName, prim.asBoolean)
            else result.addProperty(pqlName, prim.asString)
        } else {
            result.add(pqlName, value)
        }
    }

    fun extractValue(doc: JsonObject, scope: PqlScope, attribute: String): JsonElement? {
        val resolvedAttr = PqlStandardAttributes.resolve(scope, attribute)
        val cleanAttr = resolvedAttr.removePrefix("^^").removePrefix("^").removePrefix("[").removeSuffix("]")
        val prefix = when(scope) { PqlScope.LOG -> "l:"; PqlScope.TRACE -> "t:"; PqlScope.EVENT -> "e:" }

        val mappedAttr = fieldMapper.getStandardName(cleanAttr)

        if (doc.has("$prefix$cleanAttr")) return doc.get("$prefix$cleanAttr")
        if (doc.has("$prefix$mappedAttr")) return doc.get("$prefix$mappedAttr")
        if (doc.has(cleanAttr)) return doc.get(cleanAttr)
        if (doc.has(mappedAttr)) return doc.get(mappedAttr)

        val rawVal = PqlQueryExecutor.extractRobustValue(doc, scope, cleanAttr)
        if (rawVal != null) {
            return when (rawVal) {
                is JsonElement -> rawVal
                is Number -> com.google.gson.JsonPrimitive(rawVal)
                is Boolean -> com.google.gson.JsonPrimitive(rawVal)
                else -> com.google.gson.JsonPrimitive(rawVal.toString())
            }
        }

        val targetContainer = if (scope == PqlScope.LOG) doc.getAsJsonObject("log_attributes") ?: doc.getAsJsonObject("xes_attributes") else doc.getAsJsonObject("xes_attributes")
        if (targetContainer != null) {
            if (targetContainer.has(cleanAttr)) return targetContainer.get(cleanAttr)
            if (targetContainer.has(mappedAttr)) return targetContainer.get(mappedAttr)
        }

        return null
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

        val logAttrs = doc.getAsJsonObject("log_attributes")
        logAttrs?.entrySet()?.forEach { fields["l:${it.key}"] = it.value }

        return fields
    }
}
package pql.translator

import pql.model.PqlScope

data class FieldPath(val segments: List<String>) {
    fun toDotPath(): String = segments.joinToString(".")
}

class PqlFieldMapper {
    private val standardAliases = mapOf(
        "id" to "identity:id",
        "name" to "concept:name",
        "instance" to "concept:instance",
        "total" to "cost:total",
        "currency" to "cost:currency",
        "transition" to "lifecycle:transition",
        "state" to "lifecycle:state",
        "resource" to "org:resource",
        "role" to "org:role",
        "group" to "org:group",
        "timestamp" to "time:timestamp",
        "model" to "lifecycle:model",
        "version" to "xes:version",
        "features" to "xes:features"
    )

    // CZYSTA ARCHITEKTURA: Globalny dostęp do standaryzacji aliasów
    fun getStandardName(attribute: String): String {
        return standardAliases[attribute.lowercase()] ?: attribute
    }

    // CZYSTA ARCHITEKTURA: Rozwiązywanie klasyfikatorów PQL
    fun getImplicitClassifierMapping(classifierBase: String): List<String> {
        return when (classifierBase.lowercase()) {
            "event name" -> listOf("concept:name", "lifecycle:transition")
            "resource" -> listOf("org:resource")
            else -> if (classifierBase.contains("+")) classifierBase.split("+").map { it.trim() } else listOf(classifierBase)
        }
    }

    fun resolve(scope: PqlScope, attribute: String): FieldPath {
        val cleanAttribute = attribute.removePrefix("^^").removePrefix("^").removePrefix("[").removeSuffix("]")
        val lowerClean = cleanAttribute.lowercase()

        val prefixLength = when {
            lowerClean.startsWith("e:") || lowerClean.startsWith("t:") || lowerClean.startsWith("l:") || lowerClean.startsWith("c:") -> 2
            lowerClean.startsWith("classifier:") -> 11
            else -> 0
        }

        val attrLower = lowerClean.substring(prefixLength)
        val attrOriginal = cleanAttribute.substring(prefixLength)

        // Rozwiązywanie aliasu przez scentralizowaną metodę
        val normalizedAttribute = getStandardName(attrLower)

        val segments = when (scope) {
            PqlScope.LOG -> resolveLog(normalizedAttribute, attrLower)
            PqlScope.TRACE -> resolveTrace(normalizedAttribute, attrLower)
            PqlScope.EVENT -> resolveEvent(normalizedAttribute, attrLower)
        }
        return FieldPath(segments)
    }

    private fun resolveLog(originalAttr: String, lowerAttr: String): List<String> = when (lowerAttr) {
        "identity:id", "id", "_id" -> listOf("_id")
        "source" -> listOf("source")
        "importtimestamp" -> listOf("importTimestamp")
        "name", "concept:name" -> listOf("log_attributes", "concept:name")
        else -> listOf("log_attributes", originalAttr)
    }

    private fun resolveTrace(originalAttr: String, lowerAttr: String): List<String> = when (lowerAttr) {
        "identity:id", "id" -> listOf("identity:id")
        "eventids" -> listOf("eventIds")
        "logid", "logid" -> listOf("logId")
        "traceid", "traceid" -> listOf("traceId")
        "name", "concept:name" -> listOf("xes_attributes", "concept:name")
        else -> listOf("xes_attributes", originalAttr)
    }

    private fun resolveEvent(originalAttr: String, lowerAttr: String): List<String> = when (lowerAttr) {
        "identity:id", "id" -> listOf("identity:id")
        "traceid", "traceid" -> listOf("traceId")
        "logid", "logid" -> listOf("logId")
        "concept:name", "name" -> listOf("activity")
        "time:timestamp", "timestamp" -> listOf("timestamp")
        else -> {
            if (originalAttr.contains(":")) {
                val parts = originalAttr.split(":")
                listOf("xes_attributes", parts[0], parts.drop(1).joinToString(":"))
            } else {
                listOf("xes_attributes", originalAttr)
            }
        }
    }
}
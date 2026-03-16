package pql.translator

import pql.model.PqlScope

data class FieldPath(val segments: List<String>) {
    fun toDotPath(): String = segments.joinToString(".")
}

class PqlFieldMapper {

    fun resolve(scope: PqlScope, attribute: String): FieldPath {
        // POPRAWKA: Usunięcie znaków hoisting'u (^^, ^) przed mapowaniem dla bazy danych
        val cleanAttribute = attribute.removePrefix("^^").removePrefix("^")
        val segments = when (scope) {
            PqlScope.LOG -> resolveLog(cleanAttribute)
            PqlScope.TRACE -> resolveTrace(cleanAttribute)
            PqlScope.EVENT -> resolveEvent(cleanAttribute)
        }
        return FieldPath(segments)
    }

    private fun resolveLog(attribute: String): List<String> = when (attribute) {
        "_id", "id" -> listOf("_id")
        "source" -> listOf("source")
        "importTimestamp" -> listOf("importTimestamp")
        else -> listOf("log_attributes", attribute)
    }

    private fun resolveTrace(attribute: String): List<String> = when (attribute) {
        "_id", "id" -> listOf("_id")
        "eventIds" -> listOf("eventIds")
        "logId" -> listOf("logId")
        else -> listOf("xes_attributes", attribute)
    }

    private fun resolveEvent(attribute: String): List<String> = when (attribute) {
        "_id", "id" -> listOf("_id")
        "traceId" -> listOf("traceId")
        "logId" -> listOf("logId")
        "activity", "concept:name" -> listOf("activity")
        "timestamp", "time:timestamp" -> listOf("timestamp")
        else -> listOf("xes_attributes", attribute)
    }
}
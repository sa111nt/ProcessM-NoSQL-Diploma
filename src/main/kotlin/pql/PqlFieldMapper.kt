package pql

data class FieldPath(val segments: List<String>) {
    fun toDotPath(): String = segments.joinToString(".")
}

class PqlFieldMapper {

    fun resolve(scope: PqlScope, attribute: String): FieldPath {
        val segments = when (scope) {
            PqlScope.LOG -> resolveLog(attribute)
            PqlScope.TRACE -> resolveTrace(attribute)
            PqlScope.EVENT -> resolveEvent(attribute)
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
        else -> listOf("xes_attributes", attribute)
    }

    private fun resolveEvent(attribute: String): List<String> = when (attribute) {
        "_id", "id" -> listOf("_id")
        "traceId" -> listOf("traceId")
        "activity", "concept:name" -> listOf("activity")
        "timestamp", "time:timestamp" -> listOf("timestamp")
        else -> listOf("xes_attributes", attribute)
    }
}


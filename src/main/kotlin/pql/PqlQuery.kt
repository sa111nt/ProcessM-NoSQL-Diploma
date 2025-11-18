package pql

data class PqlQuery(
    val collection: PqlScope,
    val projections: List<PqlProjection>,
    val conditions: List<PqlCondition> = emptyList(),
    val orderBy: PqlOrder? = null,
    val limit: Int? = null,
    val selectAll: Boolean = false
)

data class PqlProjection(
    val scope: PqlScope?,
    val attribute: String?,
    val alias: String? = null,
    val allAttributes: Boolean = false,
    val raw: String
)

data class PqlCondition(
    val scope: PqlScope,
    val attribute: String,
    val operator: PqlOperator,
    val value: String
)

enum class PqlOperator {
    EQ,
    NEQ
}

data class PqlOrder(
    val scope: PqlScope,
    val attribute: String,
    val direction: SortDirection
)

enum class SortDirection {
    ASC,
    DESC
}

enum class PqlScope(val aliases: Set<String>, val docType: String, val label: String) {
    LOG(setOf("log", "logs", "l"), "log", "log"),
    TRACE(setOf("trace", "traces", "t"), "trace", "trace"),
    EVENT(setOf("event", "events", "e"), "event", "event");

    companion object {
        fun fromToken(token: String): PqlScope {
            val normalized = token.trim()
            return entries.firstOrNull { scope ->
                scope.aliases.any { it.equals(normalized, ignoreCase = true) }
            } ?: error("Unknown scope: '$token'")
        }
    }
}


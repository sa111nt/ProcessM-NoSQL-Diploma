package model

/**
 * Reprezentuje trace (przypadek procesu), czyli grupę zdarzeń
 * powiązanych jednym identyfikatorem caseId / traceId.
 */

data class Trace(
    val id: String,

    val attributes: MutableMap<String, Any?> = mutableMapOf(),

    val events: MutableList<Event> = mutableListOf()
)
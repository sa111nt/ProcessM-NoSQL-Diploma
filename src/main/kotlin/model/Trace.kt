package model

import java.util.UUID

/**
 * Reprezentuje trace (przypadek procesu), czyli grupę zdarzeń
 * powiązanych jednym identyfikatorem caseId / traceId.
 * Zgodnie ze standardem XES, identity:id musi być typu UUID.
 */
data class Trace(
    val id: UUID,

    val attributes: MutableMap<String, Any?> = mutableMapOf(),

    val events: MutableList<Event> = mutableListOf()
)
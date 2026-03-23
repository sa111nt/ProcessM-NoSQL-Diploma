package model

import java.util.UUID

/**
 * Reprezentuje pojedyncze zdarzenie (Event) w logu procesowym.
 * Jest to podstawowy element danych w XES/OCEL.
 * Zgodnie ze standardem XES, identity:id musi być typu UUID.
 */
data class Event(
    val id: UUID,

    val traceId: UUID? = null,

    val name: String? = null,

    val timestamp: String? = null,

    val attributes: Map<String, Any?> = emptyMap(),

    val relatedObjects: Map<String, List<String>> = emptyMap()
)
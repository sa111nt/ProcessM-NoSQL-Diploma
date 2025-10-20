package model

/**
 * Reprezentuje pojedyncze zdarzenie (Event) w logu procesowym.
 * Jest to podstawowy element danych w XES/OCEL.
 */
data class Event(
    val id: String,

    val traceId: String? = null,

    val name: String? = null,

    val timestamp: String? = null,

    val attributes: Map<String, Any?> = emptyMap()
)
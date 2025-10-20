package model

/**
 * Reprezentuje cały dziennik zdarzeń.
 * Jest najwyższym poziomem modelu danych (scope: log).
 */
data class EventLog(
    val events: List<Event> = emptyList(),

    val traces: List<Trace> = emptyList(),

    val source: String? = null,

    val importTimestamp: Long = System.currentTimeMillis(),

    val attributes: Map<String, Any?> = emptyMap()
)
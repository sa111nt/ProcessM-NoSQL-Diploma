package model

data class Log(
    val events: List<Event> = emptyList(),
    val traces: List<Trace> = emptyList(),
    val source: String? = null,
    val importTimestamp: Long = System.currentTimeMillis(),
    val attributes: Map<String, Any?> = emptyMap(),
    val types: Map<String, String> = emptyMap()
)
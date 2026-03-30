package model

import java.util.UUID

data class Event(
    val id: UUID,
    val traceId: UUID? = null,
    val name: String? = null,
    val timestamp: String? = null,
    val attributes: Map<String, Any?> = emptyMap(),
    val types: Map<String, String> = emptyMap(),
    val relatedObjects: Map<String, List<String>> = emptyMap()
)
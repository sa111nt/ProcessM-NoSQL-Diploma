package model

import java.util.UUID

data class Trace(
    val id: UUID,
    val attributes: MutableMap<String, Any?> = mutableMapOf(),
    val events: MutableList<Event> = mutableListOf()
)
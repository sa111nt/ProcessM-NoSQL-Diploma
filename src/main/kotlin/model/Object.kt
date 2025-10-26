package model

data class ProcessObject(
    val id: String,
    val type: String,
    val attributes: Map<String, Any?> = emptyMap()
)
package input.parsers

import model.Trace

data class XesExtension(val name: String, val prefix: String, val uri: String)
data class XesGlobal(val scope: String, val attributes: Map<String, Any?>)
data class XesClassifier(val name: String, val keys: List<String>)

sealed class XESComponent

data class XESLogAttributes(
    val attributes: Map<String, Any?>,
    val extensions: List<XesExtension> = emptyList(),
    val globals: List<XesGlobal> = emptyList(),
    val classifiersDef: List<XesClassifier> = emptyList(),
    val classifiers: Map<String, List<String>> = emptyMap(),
    val types: Map<String, String>
) : XESComponent()

data class XESTrace(val trace: Trace) : XESComponent()
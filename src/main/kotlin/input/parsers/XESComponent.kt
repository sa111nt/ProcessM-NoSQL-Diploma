package input.parsers

import model.Trace

// 1. SEALED CLASS (Klasa zapieczętowana)
// To działa jak "super-interfejs". Mówi: "Każdy element strumienia XML
// będzie albo atrybutami logu, albo śladem (Trace)".
// Dzięki 'sealed' kompilator wie, że nie ma innych opcji, co ułatwia użycie 'when'.
sealed class XESComponent

// 2. LOG ATTRIBUTES (Nagłówek pliku)
// Ten obiekt pojawia się w strumieniu TYLKO RAZ (zazwyczaj na początku).
// Zawiera globalne informacje o logu (np. autor, data utworzenia, nazwa systemu).
data class XESLogAttributes(
    val attributes: Map<String, Any?>,
    val classifiers: Map<String, List<String>> = emptyMap()
) : XESComponent()

// 3. TRACE (Ciało pliku)
// Ten obiekt pojawia się w strumieniu TYSIĄCE RAZY.
// Każdy XESTrace to jeden pełny przebieg procesu (zawierający listę Eventów).
data class XESTrace(val trace: Trace) : XESComponent()
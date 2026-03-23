package pql.model

/**
 * Zapewnia mapowanie skrótowych nazw atrybutów PQL (shorthands)
 * na pełne standardowe nazwy zgodne ze standardem IEEE 1849-2016 (XES).
 */
object PqlStandardAttributes {

    /**
     * Mapuje podany atrybut (np. "name", "total", "timestamp") na jego pełną postać ("concept:name", "cost:total").
     * Jeśli atrybut jest już w pełnej postaci lub jest niestandardowy, zwraca go bez zmian.
     */
    fun resolve(scope: PqlScope, attribute: String): String {
        // Zdejmujemy daszki hoistingu do sprawdzenia samej nazwy
        val cleanAttr = attribute.removePrefix("^^").removePrefix("^").lowercase()

        // Klasyfikatory (np. c:Resource, classifier:activity) zostawiamy w spokoju
        if (cleanAttr.startsWith("c:") || cleanAttr.startsWith("classifier:")) {
            return attribute
        }

        // Tłumaczenie skrótów PQL na ustandaryzowane nazwy XES
        val resolvedAttr = when (cleanAttr) {
            // Wspólne dla wielu poziomów (Log, Trace, Event)
            "name" -> "concept:name"
            "id" -> "identity:id"
            "currency" -> "cost:currency"
            "total" -> "cost:total"

            // Specyficzne dla poziomu Log
            "model" -> "lifecycle:model"
            "version" -> "xes:version"
            "features" -> "xes:features"

            // Specyficzne dla poziomu Event
            "instance" -> "concept:instance"
            "transition" -> "lifecycle:transition"
            "state" -> "lifecycle:state"
            "resource" -> "org:resource"
            "role" -> "org:role"
            "group" -> "org:group"
            "timestamp" -> "time:timestamp"

            // Jeśli to już jest pełna nazwa (np. "concept:name") lub atrybut niestandardowy
            else -> attribute
        }

        // Przywracamy znaczniki hoistingu (^ lub ^^), jeśli były w zapytaniu
        val hoistingPrefix = attribute.takeWhile { it == '^' }

        // Zwracamy oryginał dla nieznanych, chyba że zmapowaliśmy na coś nowego
        return if (resolvedAttr == cleanAttr) attribute else hoistingPrefix + resolvedAttr
    }

    /**
     * Zwraca listę wszystkich standardowych, wbudowanych atrybutów dla danego poziomu.
     */
    fun getStandardAttributesFor(scope: PqlScope): List<String> {
        return when (scope) {
            PqlScope.LOG -> listOf(
                "concept:name", "identity:id", "lifecycle:model", "xes:version", "xes:features"
            )
            PqlScope.TRACE -> listOf(
                "concept:name", "cost:currency", "cost:total", "identity:id"
            )
            PqlScope.EVENT -> listOf(
                "concept:name", "concept:instance", "cost:currency", "cost:total",
                "identity:id", "lifecycle:transition", "lifecycle:state",
                "org:resource", "org:role", "org:group", "time:timestamp"
            )
        }
    }
}
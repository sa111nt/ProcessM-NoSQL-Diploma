package input.parsers

import model.Event
import model.Trace
import java.io.File
import java.util.UUID
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants
import javax.xml.stream.XMLStreamReader

// Interfejs definiujący nasz parser jako Iterator.
// Dzięki temu możemy używać go w pętli 'for' lub w 'mapper.map(parser)'.
interface XESInputStream : Iterator<XESComponent>

class StaxXesParser(private val reader: XMLStreamReader) : XESInputStream {

    // Konstruktor pomocniczy dla plików
    constructor(file: File) : this(
        XMLInputFactory.newDefaultFactory().createXMLStreamReader(file.inputStream())
    )

    // Bufor na następny element.
    // Parser działa w trybie "pull" (wyciągania), więc musimy zajrzeć do przodu,
    // co jest w pliku, zapisać to tutaj i oddać, gdy ktoś zawoła next().
    private var nextComponent: XESComponent? = null

    // POPRAWKA: Lista obsługiwanych typów atrybutów XES.
    // Dodano 'id' zgodnie z wymaganiami dokumentacji PQL.
    private val supportedTypes = setOf("string", "date", "int", "float", "boolean", "id")

    // 1. LOGIKA GŁÓWNA ITERATORA
    // Ta metoda decyduje, czy czytać dalej, czy skończyć.
    override fun hasNext(): Boolean {
        // Jeśli mamy coś w buforze, to znaczy, że jest co zwracać.
        if (nextComponent != null) return true

        // Tutaj był error
        // Sprawdzamy, czy kursor parsera nie zatrzymał się już na tagu <trace>
        // (np. po wyjściu z metody parseLogAttributes).
        // Jeśli tak, parsujemy ślad OD RAZU, bez wywoływania reader.next(),
        // które przesunęłoby nas za daleko i zgubiłoby pierwszy ślad.
        if (reader.eventType == XMLStreamConstants.START_ELEMENT && reader.localName == "trace") {
            nextComponent = XESTrace(parseTrace())
            return true
        }

        // Pętla czyta surowy XML, dopóki nie znajdzie czegoś ciekawego (Log lub Trace)
        while (reader.hasNext()) {
            when (reader.next()) {
                XMLStreamConstants.START_ELEMENT -> {
                    val name = reader.localName

                    // Jeśli trafiliśmy na początek pliku <log> -> parsujemy atrybuty globalne
                    if (name == "log") {
                        nextComponent = XESLogAttributes(parseLogAttributes())
                        return true
                    }
                    // Jeśli trafiliśmy na <trace> -> parsujemy cały ślad wraz ze zdarzeniami
                    else if (name == "trace") {
                        nextComponent = XESTrace(parseTrace())
                        return true
                    }
                }
                // Koniec pliku XML -> kończymy iterator
                XMLStreamConstants.END_DOCUMENT -> return false
            }
        }
        return false
    }

    // Zwraca przygotowany wcześniej element i czyści bufor
    override fun next(): XESComponent {
        if (nextComponent != null || hasNext()) {
            val result = nextComponent!!
            nextComponent = null
            return result
        }
        throw NoSuchElementException()
    }

    // 2. PARSOWANIE ATRYBUTÓW LOGU (Nagłówek)
    // Czyta wszystko wewnątrz <log>...</log> ZANIM wystąpi pierwszy <trace>.
    private fun parseLogAttributes(): Map<String, Any?> {
        val attributes = mutableMapOf<String, Any?>()
        while (reader.hasNext()) {
            reader.next() // Przesuwamy kursor

            if (reader.isStartElement) {
                val name = reader.localName

                // POPRAWKA: Używamy zbioru supportedTypes zamiast wpisywać typy ręcznie.
                // XES definiuje typy danych jako tagi: <string key="..." value="..."/>
                if (name in supportedTypes) {
                    val key = reader.getAttributeValue(null, "key")
                    val value = reader.getAttributeValue(null, "value")
                    attributes[key] = castValue(name, value)
                }
                // Jeśli trafimy na <trace>, to znaczy, że atrybuty logu się skończyły.
                // Przerywamy i zwracamy atrybuty. Kursor zostaje na tagu <trace>,
                // co zostanie obsłużone przez poprawiony 'hasNext()'.
                else if (name == "trace") {
                    return attributes
                }
            } else if (reader.isEndElement && reader.localName == "log") return attributes
        }
        return attributes
    }

    // 3. PARSOWANIE ŚLADU (Trace)
    // To jest wywoływane tysiące razy. Tworzy obiekt Trace zawierający listę Eventów.
    private fun parseTrace(): Trace {
        val traceAttributes = mutableMapOf<String, Any?>()
        val events = mutableListOf<Event>()
        var traceId: String? = null

        // Czytamy XML wewnątrz tagu <trace>...</trace>
        while (reader.hasNext()) {
            reader.next()
            if (reader.isStartElement) {
                val name = reader.localName

                // POPRAWKA: Używamy supportedTypes.
                // Atrybuty samego śladu (np. concept:name to zazwyczaj ID śladu)
                if (name in supportedTypes) {
                    val key = reader.getAttributeValue(null, "key")
                    val value = castValue(name, reader.getAttributeValue(null, "value"))

                    // Wyciągamy ID śladu
                    if (key == "concept:name") traceId = value?.toString()
                    traceAttributes[key] = value
                }
                // Jeśli trafimy na <event>, odpalamy parser zdarzenia
                else if (name == "event") {
                    // Przekazujemy traceId do eventu, żeby wiedział do kogo należy (Relacyjność)
                    events.add(parseEvent(traceId ?: "unknown_trace"))
                }
            }
            // Koniec tagu </trace> -> przerywamy pętlę i zwracamy gotowy obiekt
            else if (reader.isEndElement && reader.localName == "trace") break
        }

        return Trace(
            // ZMIANA: UUID zapewnia unikalność, nawet jak w pliku XES brakuje ID
            id = traceId ?: "trace_${UUID.randomUUID()}",
            attributes = traceAttributes,
            events = events
        )
    }

    // 4. PARSOWANIE ZDARZENIA (Event)
    // Czyta pojedynczy tag <event>...</event>
    private fun parseEvent(traceId: String): Event {
        val attrs = mutableMapOf<String, Any?>()
        var name: String? = null
        var timestamp: String? = null
        var eventId: String? = null

        var reading = true
        while (reader.hasNext() && reading) {
            reader.next()
            if (reader.isStartElement) {
                val tag = reader.localName

                // POPRAWKA: Używamy supportedTypes.
                // Wyciąganie atrybutów zdarzenia (nazwa czynności, czas, koszt itp.)
                if (tag in supportedTypes) {
                    val key = reader.getAttributeValue(null, "key")
                    val value = castValue(tag, reader.getAttributeValue(null, "value"))

                    // Mapowanie kluczowych pól standardu XES
                    when (key) {
                        "concept:name" -> name = value?.toString()
                        "time:timestamp" -> timestamp = value?.toString()
                        "id" -> eventId = value?.toString()
                    }
                    attrs[key] = value
                }
            }
            // Koniec tagu </event>
            else if (reader.isEndElement && reader.localName == "event") reading = false
        }

        return Event(
            // ZMIANA: Generujemy unikalne ID: TraceID + UUID, żeby uniknąć kolizji w bazie
            id = eventId ?: "${traceId}_event_${UUID.randomUUID()}",
            traceId = traceId,
            name = name,
            timestamp = timestamp,
            attributes = attrs
        )
    }

    // 5. KONWERSJA TYPÓW (Helper)
    // Standard XES definiuje typy w XMLu (np. <int value="5"/>).
    // Musimy to zamienić ze Stringa na typy Kotlina/Javy.
    private fun castValue(tag: String, value: String?): Any? {
        if (value == null) return null
        return when (tag) {
            // POPRAWKA: 'id' oraz 'date' traktujemy jako String (Baza NoSQL/JSON tak to przechowuje)
            "string", "date", "id" -> value
            "int" -> value.toLongOrNull()
            "float" -> value.toDoubleOrNull()
            "boolean" -> value.toBooleanStrictOrNull()
            else -> value
        }
    }
}
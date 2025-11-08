package input.parsers

import model.Event
import model.Trace
import model.EventLog
import java.io.File
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants
import javax.xml.stream.XMLStreamReader
import java.util.UUID

// Definiujemy interfejs XESInputStream, żeby móc łatwo iterować (jak w drugim projekcie)
interface XESInputStream : Iterator<Any> // Zastąp Any rzeczywistymi klasami Log/Trace/Event

// Użyjemy koncepcji StAX (pull parser), bo jest to bardziej nowoczesna alternatywa dla SAX
class StaxXesParser(private val reader: XMLStreamReader) : XESInputStream {

    constructor(file: File) : this(
        XMLInputFactory.newDefaultFactory().createXMLStreamReader(file.inputStream())
    )

    private var nextComponent: Any? = null // Używamy Any jako uproszczenia

    override fun hasNext(): Boolean {
        if (nextComponent != null) return true

        // Szukaj następnego elementu, który może być Log, Trace lub Event
        while (reader.hasNext()) {
            when (reader.next()) {
                XMLStreamConstants.START_ELEMENT -> {
                    val localName = reader.localName

                    if (localName == "log") {
                        // Dla uproszczenia zwracamy tylko atrybuty logu, a nie cały Log
                        nextComponent = parseLogAttributes()
                        return true
                    } else if (localName == "trace") {
                        nextComponent = parseTrace()
                        return true
                    }
                    // Możesz tu dodać parsowanie eventów, jeśli chcesz je streamować bez śladów
                }
                XMLStreamConstants.END_DOCUMENT -> return false
            }
        }
        return false
    }

    override fun next(): Any {
        if (nextComponent != null || hasNext()) {
            val result = nextComponent!!
            nextComponent = null
            return result
        }
        throw NoSuchElementException()
    }

    // Zaimplementuj funkcję do parsowania atrybutów logu
    private fun parseLogAttributes(): Map<String, Any?> {
        val attributes = mutableMapOf<String, Any?>()

        while (reader.hasNext()) {
            reader.next()
            if (reader.isStartElement) {
                // Ta sekcja musi czytać tagi atrybutów: <string>, <date> itp.
                if (reader.localName in setOf("string", "date", "int", "float", "boolean")) {
                    val key = reader.getAttributeValue(null, "key")
                    val valueString = reader.getAttributeValue(null, "value")
                    attributes[key] = castValue(reader.localName, valueString)
                } else if (reader.localName == "trace") {
                    // Osiągnęliśmy pierwszy <trace>, więc wychodzimy, by zwrócić Log
                    return attributes
                }
            } else if (reader.isEndElement && reader.localName == "log") {
                return attributes
            }
        }
        return attributes
    }

    // Zaimplementuj funkcję do parsowania pojedynczego śladu (Trace)
    private fun parseTrace(): Trace {
        val traceAttributes = mutableMapOf<String, Any?>()
        val events = mutableListOf<Event>()
        var traceId: String? = null

        while (reader.hasNext()) {
            reader.next()

            if (reader.isStartElement) {
                val localName = reader.localName
                if (localName in setOf("string", "date", "int", "float", "boolean")) {
                    val key = reader.getAttributeValue(null, "key")
                    val valueString = reader.getAttributeValue(null, "value")
                    val value = castValue(localName, valueString)

                    if (key == "concept:name") traceId = value?.toString()
                    traceAttributes[key] = value

                } else if (localName == "event") {
                    events.add(parseEvent(traceId ?: "unknown"))
                }
            } else if (reader.isEndElement && reader.localName == "trace") {
                // Zakończyliśmy parsowanie śladu
                break
            }
        }

        return Trace(
            id = traceId ?: "trace_${System.currentTimeMillis()}", // Lepsze ID
            attributes = traceAttributes,
            events = events
        )
    }

    // Zaimplementuj funkcję do parsowania pojedynczego zdarzenia (Event)
    private fun parseEvent(traceId: String): Event {
        val eventAttributes = mutableMapOf<String, Any?>()
        var eventName: String? = null
        var timestamp: String? = null
        var eventId: String? = null

        // Przeskakujemy na następny element, aby zacząć czytać atrybuty <event>
        var isReadingAttributes = true

        while (reader.hasNext() && isReadingAttributes) {
            reader.next()

            if (reader.isStartElement) {
                val localName = reader.localName
                if (localName in setOf("string", "date", "int", "float", "boolean")) {
                    val key = reader.getAttributeValue(null, "key")
                    val valueString = reader.getAttributeValue(null, "value")
                    val value = castValue(localName, valueString)

                    when (key) {
                        "concept:name" -> eventName = value?.toString()
                        "time:timestamp" -> timestamp = value?.toString()
                        "id" -> eventId = value?.toString()
                    }
                    eventAttributes[key] = value

                } else {
                    // Nieoczekiwany tag wewnątrz eventu, obsługa błędu lub ignorowanie
                }
            } else if (reader.isEndElement && reader.localName == "event") {
                isReadingAttributes = false
            }
        }

        return Event(
            id = eventId ?: "${traceId}_event_${UUID.randomUUID()}",
            traceId = traceId,
            name = eventName,
            timestamp = timestamp,
            attributes = eventAttributes
        )
    }

    // Przenieś logikę konwersji typów
    private fun castValue(tagName: String, valueString: String?): Any? {
        if (valueString == null) return null
        return when (tagName) {
            "string", "date" -> valueString
            "int" -> valueString.toLongOrNull()
            "float" -> valueString.toDoubleOrNull()
            "boolean" -> valueString.toBooleanStrictOrNull()
            else -> null
        }
    }

    // Twoja funkcja `parse` musiałaby zostać zmieniona, aby korzystać z tej klasy iteracyjnie:
    fun parseFileToLog(file: File): EventLog {
        val staxParser = StaxXesParser(file)
        val allEvents = mutableListOf<Event>()
        val traces = mutableListOf<Trace>()
        var logAttributes: Map<String, Any?>? = null

        // Iterujemy przez komponenty (Log Attributes, Trace, Event...)
        staxParser.forEach { component ->
            when (component) {
                is Map<*, *> -> logAttributes = component as Map<String, Any?>
                is Trace -> {
                    traces.add(component)
                    allEvents.addAll(component.events)
                }
                // Opcjonalnie: Obsługa pojedynczych Eventów poza Trace
                // is Event -> allEvents.add(component)
            }
        }

        return EventLog(
            events = allEvents,
            traces = traces,
            attributes = logAttributes ?: emptyMap()
        )
    }
}
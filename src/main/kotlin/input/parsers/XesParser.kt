package input.parsers

import model.Event
import model.Trace
import java.io.File
import java.util.UUID
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants
import javax.xml.stream.XMLStreamReader

interface XESInputStream : Iterator<XESComponent>

class StaxXesParser(private val reader: XMLStreamReader) : XESInputStream {

    constructor(file: File) : this(
        XMLInputFactory.newDefaultFactory().createXMLStreamReader(file.inputStream())
    )

    private var nextComponent: XESComponent? = null
    private val primitiveTypes = setOf("string", "date", "int", "float", "boolean", "id")

    override fun hasNext(): Boolean {
        if (nextComponent != null) return true

        if (reader.eventType == XMLStreamConstants.START_ELEMENT && reader.localName == "trace") {
            nextComponent = XESTrace(parseTrace())
            return true
        }

        while (reader.hasNext()) {
            when (reader.next()) {
                XMLStreamConstants.START_ELEMENT -> {
                    val name = reader.localName
                    if (name == "log") {
                        nextComponent = XESLogAttributes(parseLogAttributes())
                        return true
                    } else if (name == "trace") {
                        nextComponent = XESTrace(parseTrace())
                        return true
                    }
                }
                XMLStreamConstants.END_DOCUMENT -> return false
            }
        }
        return false
    }

    override fun next(): XESComponent {
        if (nextComponent != null || hasNext()) {
            val result = nextComponent!!
            nextComponent = null
            return result
        }
        throw NoSuchElementException()
    }

    // SPŁASZCZAJĄCA REKURENCJA: Generuje ultra-lekkie klucze dla bazy NoSQL
    private fun parseAttribute(prefix: String = ""): List<Pair<String, Any?>> {
        val tag = reader.localName
        val rawKey = reader.getAttributeValue(null, "key") ?: "unknown"
        val key = if (prefix.isEmpty()) rawKey else "$prefix:$rawKey"
        val results = mutableListOf<Pair<String, Any?>>()

        if (tag in primitiveTypes) {
            val value = castValue(tag, reader.getAttributeValue(null, "value"))
            results.add(key to value)
        }

        while (reader.hasNext()) {
            reader.next()
            if (reader.isStartElement) {
                val childTag = reader.localName
                if (childTag in primitiveTypes || childTag == "list" || childTag == "container") {
                    results.addAll(parseAttribute(key))
                }
            } else if (reader.isEndElement && reader.localName == tag) {
                break
            }
        }
        return results
    }

    private fun parseLogAttributes(): Map<String, Any?> {
        val attributes = mutableMapOf<String, Any?>()
        while (reader.hasNext()) {
            reader.next()
            if (reader.isStartElement) {
                val name = reader.localName
                if (name in primitiveTypes || name == "list" || name == "container") {
                    parseAttribute().forEach { attributes[it.first] = it.second }
                } else if (name == "trace") {
                    return attributes
                }
            } else if (reader.isEndElement && reader.localName == "log") return attributes
        }
        return attributes
    }

    private fun parseTrace(): Trace {
        val traceAttributes = mutableMapOf<String, Any?>()
        val events = mutableListOf<Event>()

        var currentTraceId = UUID.randomUUID()

        while (reader.hasNext()) {
            reader.next()
            if (reader.isStartElement) {
                val name = reader.localName
                if (name in primitiveTypes || name == "list" || name == "container") {
                    parseAttribute().forEach { attr ->
                        traceAttributes[attr.first] = attr.second
                        if (attr.first == "identity:id" && attr.second is UUID) {
                            currentTraceId = attr.second as UUID
                        }
                    }
                } else if (name == "event") {
                    events.add(parseEvent(currentTraceId))
                }
            } else if (reader.isEndElement && reader.localName == "trace") break
        }

        val finalizedEvents = events.map { it.copy(traceId = currentTraceId) }.toMutableList()

        return Trace(
            id = currentTraceId,
            attributes = traceAttributes,
            events = finalizedEvents
        )
    }

    private fun parseEvent(traceId: UUID): Event {
        val attrs = mutableMapOf<String, Any?>()
        var name: String? = null
        var timestamp: String? = null
        var eventId: UUID? = null

        var reading = true
        while (reader.hasNext() && reading) {
            reader.next()
            if (reader.isStartElement) {
                val tag = reader.localName
                if (tag in primitiveTypes || tag == "list" || tag == "container") {
                    parseAttribute().forEach { attr ->
                        when (attr.first) {
                            "concept:name" -> name = attr.second?.toString()
                            "time:timestamp" -> timestamp = attr.second?.toString()
                            "identity:id" -> if (attr.second is UUID) eventId = attr.second as UUID
                        }
                        attrs[attr.first] = attr.second
                    }
                }
            } else if (reader.isEndElement && reader.localName == "event") reading = false
        }

        return Event(
            id = eventId ?: UUID.randomUUID(),
            traceId = traceId,
            name = name,
            timestamp = timestamp,
            attributes = attrs
        )
    }

    private fun castValue(tag: String, value: String?): Any? {
        if (value == null) return null
        return try {
            when (tag) {
                "string", "date" -> value
                "id" -> UUID.fromString(value)
                "int" -> value.toLong()
                "float" -> value.toDouble()
                "boolean" -> value.toBooleanStrict()
                else -> value
            }
        } catch (e: Exception) {
            if (tag == "id") null else value
        }
    }
}
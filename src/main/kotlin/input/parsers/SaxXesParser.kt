package input.parsers

import model.Event
import model.Trace
import model.EventLog
import org.xml.sax.Attributes
import org.xml.sax.helpers.DefaultHandler
import java.io.File
import java.util.UUID
import javax.xml.parsers.SAXParserFactory

class SaxXesParser : DefaultHandler() {

    private val traces = mutableListOf<Trace>()
    private val events = mutableListOf<Event>()
    private val logAttributes = mutableMapOf<String, Any?>()

    private var currentTraceId: String? = null
    private var currentTraceAttributes = mutableMapOf<String, Any?>()
    private var currentTraceEvents = mutableListOf<Event>()

    private var currentEventAttributes = mutableMapOf<String, Any?>()
    private var currentEventName: String? = null
    private var currentTimestamp: String? = null

    private var insideTrace = false
    private var insideEvent = false

    fun parse(file: File): EventLog {
        val factory = SAXParserFactory.newInstance()
        val parser = factory.newSAXParser()
        parser.parse(file, this)

        return EventLog(
            events = events,
            traces = traces,
            attributes = logAttributes,
            source = file.nameWithoutExtension
        )
    }

    override fun startElement(uri: String?, localName: String?, qName: String, attributes: Attributes) {
        when (qName) {
            "trace" -> {
                insideTrace = true
                currentTraceId = "trace_${UUID.randomUUID()}"
                currentTraceAttributes = mutableMapOf()
                currentTraceEvents = mutableListOf()
            }

            "event" -> {
                insideEvent = true
                currentEventAttributes = mutableMapOf()
                currentEventName = null
                currentTimestamp = null
            }

            "string", "date", "int", "float", "boolean" -> {
                val key = attributes.getValue("key")
                val value = castValue(qName, attributes.getValue("value"))

                when {
                    insideEvent -> {
                        currentEventAttributes[key] = value
                        if (key == "concept:name") currentEventName = value?.toString()
                        if (key == "time:timestamp") currentTimestamp = value?.toString()
                    }

                    insideTrace -> {
                        currentTraceAttributes[key] = value
                    }

                    else -> {
                        logAttributes[key] = value
                    }
                }
            }
        }
    }

    override fun endElement(uri: String?, localName: String?, qName: String) {
        when (qName) {
            "event" -> {
                val event = Event(
                    id = "event_${UUID.randomUUID()}",
                    traceId = currentTraceId,
                    name = currentEventName,
                    timestamp = currentTimestamp,
                    attributes = currentEventAttributes.toMap()
                )
                events.add(event)
                currentTraceEvents.add(event)
                insideEvent = false
            }

            "trace" -> {
                val trace = Trace(
                    id = currentTraceId ?: "trace_${UUID.randomUUID()}",
                    attributes = currentTraceAttributes,
                    events = currentTraceEvents
                )
                traces.add(trace)
                insideTrace = false
            }
        }
    }

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
}
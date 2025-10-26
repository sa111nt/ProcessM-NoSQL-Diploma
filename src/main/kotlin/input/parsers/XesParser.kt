package input.parsers

import model.Event
import model.Trace
import model.EventLog
import org.w3c.dom.Element
import org.w3c.dom.NodeList
import java.io.File
import javax.xml.parsers.DocumentBuilderFactory

class XesParser {

    fun parse(file: File): EventLog {
        val factory = DocumentBuilderFactory.newInstance()
        val builder = factory.newDocumentBuilder()
        val document = builder.parse(file)
        document.documentElement.normalize()

        val logElement = document.documentElement

        val logAttributes = parseAttributes(logElement)

        val traceNodes = logElement.getElementsByTagName("trace")
        val traces = mutableListOf<Trace>()
        val allEvents = mutableListOf<Event>()

        for (t in 0 until traceNodes.length) {
            val traceNode = traceNodes.item(t) as Element

            val traceAttributes = parseAttributes(traceNode)

            val traceId = traceAttributes["concept:name"]?.toString() ?: "trace_${t}"

            val eventsForTrace = mutableListOf<Event>()
            val eventNodes = traceNode.getElementsByTagName("event")

            for (i in 0 until eventNodes.length) {
                val eventNode = eventNodes.item(i) as Element

                val eventAttributes = parseAttributes(eventNode)

                val eventName = eventAttributes["concept:name"]?.toString()
                val timestamp = eventAttributes["time:timestamp"]?.toString()

                val eventId = eventAttributes["id"]?.toString() ?: "${traceId}_event_${i}"

                val event = Event(
                    id = eventId,
                    traceId = traceId,
                    name = eventName,
                    timestamp = timestamp,
                    attributes = eventAttributes
                )
                eventsForTrace.add(event)
                allEvents.add(event)
            }

            traces.add(
                Trace(
                    id = traceId,
                    attributes = traceAttributes,
                    events = eventsForTrace
                )
            )
        }

        return EventLog(
            events = allEvents,
            traces = traces,
            attributes = logAttributes
        )
    }

    private fun parseAttributes(element: Element): MutableMap<String, Any?> {
        val attributes = mutableMapOf<String, Any?>()
        val children = element.childNodes

        for (i in 0 until children.length) {
            val node = children.item(i)

            if (node.nodeType == org.w3c.dom.Node.ELEMENT_NODE) {
                val attrElement = node as Element
                val tagName = attrElement.tagName

                when (tagName) {
                    "string", "date", "int", "float", "boolean" -> {
                        val key = attrElement.getAttribute("key")
                        val valueString = attrElement.getAttribute("value")

                        val value: Any? = when (tagName) {
                            "string", "date" -> valueString
                            "int" -> valueString.toLongOrNull()
                            "float" -> valueString.toDoubleOrNull()
                            "boolean" -> valueString.toBooleanStrictOrNull()
                            else -> null
                        }

                        if (key.isNotEmpty() && value != null) {
                            attributes[key] = value
                        }
                    }
                }
            }
        }
        return attributes
    }
}
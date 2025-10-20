package input.parsers

import model.Event
import model.Trace
import model.EventLog
import org.w3c.dom.Element
import java.io.File
import javax.xml.parsers.DocumentBuilderFactory

class XesParser {

    fun parse(file: File): EventLog {
        val factory = DocumentBuilderFactory.newInstance()
        val builder = factory.newDocumentBuilder()
        val document = builder.parse(file)
        document.documentElement.normalize()

        val traceNodes = document.getElementsByTagName("trace")
        val traces = mutableListOf<Trace>()
        val allEvents = mutableListOf<Event>()

        for (t in 0 until traceNodes.length) {
            val traceNode = traceNodes.item(t) as Element

            val traceAttributes = mutableMapOf<String, Any?>()
            val traceAttrNodes = traceNode.getElementsByTagName("*")
            for (i in 0 until traceAttrNodes.length) {
                val node = traceAttrNodes.item(i) as Element
                if (node.tagName != "event") {
                    val key = node.getAttribute("key")
                    val value = node.getAttribute("value")
                    traceAttributes[key] = value
                }
            }

            val traceId = traceAttributes["concept:name"]?.toString() ?: "trace_$t"
            val eventNodes = traceNode.getElementsByTagName("event")
            val eventsForTrace = mutableListOf<Event>()

            for (i in 0 until eventNodes.length) {
                val eventNode = eventNodes.item(i) as Element
                val attributes = mutableMapOf<String, Any?>()

                val children = eventNode.getElementsByTagName("*")
                for (j in 0 until children.length) {
                    val attrNode = children.item(j) as Element
                    val key = attrNode.getAttribute("key")
                    val value = attrNode.getAttribute("value")
                    attributes[key] = value
                }

                val eventId = attributes["concept:name"]?.toString() ?: "event_${t}_$i"
                val event = Event(
                    id = eventId,
                    traceId = traceId,
                    attributes = attributes
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
            traces = traces
        )
    }
}
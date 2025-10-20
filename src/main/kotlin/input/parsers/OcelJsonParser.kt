package input.parsers

import com.google.gson.Gson
import com.google.gson.JsonObject
import java.io.File
import model.Event
import model.Trace
import model.EventLog

class OcelJsonParser {

    private val gson = Gson()

    fun parse(file: File): EventLog {
        val text = file.readText()
        val jsonObj = gson.fromJson(text, JsonObject::class.java)

        val eventsJson = jsonObj.getAsJsonObject("events")
        val events = mutableListOf<Event>()
        val traces = mutableListOf<Trace>()

        for ((eventId, eventData) in eventsJson.entrySet()) {
            val evObj = eventData.asJsonObject
            val attrs = mutableMapOf<String, Any?>()

            for ((k, v) in evObj.entrySet()) {
                attrs[k] = when {
                    v.isJsonPrimitive && v.asJsonPrimitive.isString -> v.asString
                    v.isJsonPrimitive && v.asJsonPrimitive.isNumber -> v.asNumber
                    v.isJsonPrimitive && v.asJsonPrimitive.isBoolean -> v.asBoolean
                    else -> v.toString()
                }
            }

            val traceId = attrs["ocel:activity"]?.toString() ?: "trace_default"
            val event = Event(id = eventId, traceId = traceId, attributes = attrs)
            events.add(event)

            var trace = traces.find { it.id == traceId }
            if (trace == null) {
                trace = Trace(id = traceId)
                traces.add(trace)
            }
            trace.events.add(event)
        }

        return EventLog(events = events, traces = traces)
    }
}
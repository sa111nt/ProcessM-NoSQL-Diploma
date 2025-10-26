package input.parsers

import com.google.gson.Gson
import com.google.gson.JsonObject
import java.io.File
import model.Event
import model.ProcessObject
import model.EventLog

class OcelJsonParser {
    private val gson = Gson()

    fun parse(file: File): EventLog {
        val text = file.readText()
        val jsonObj = gson.fromJson(text, JsonObject::class.java)

        val objectsJson = jsonObj.getAsJsonObject("objects")
        val processObjects = mutableListOf<ProcessObject>()
        for ((objectId, objectData) in objectsJson.entrySet()) {
            val obj = objectData.asJsonObject
            val type = obj.getAsJsonPrimitive("ocel:type").asString
            val ovmap = obj.getAsJsonObject("ocel:ovmap")

            val attrs = mutableMapOf<String, Any?>()

            processObjects.add(ProcessObject(id = objectId, type = type, attributes = attrs))
        }

        val eventsJson = jsonObj.getAsJsonObject("events")
        val events = mutableListOf<Event>()

        for ((eventId, eventData) in eventsJson.entrySet()) {
            val evObj = eventData.asJsonObject
            val vmap = evObj.getAsJsonObject("ocel:vmap")
            val omap = evObj.getAsJsonObject("ocel:omap")

            val name = evObj.getAsJsonPrimitive("ocel:activity")?.asString
            val timestamp = evObj.getAsJsonPrimitive("ocel:timestamp")?.asString

            val attrs = mutableMapOf<String, Any?>()

            val relatedObjects = mutableMapOf<String, List<String>>()
            for ((objType, objIdsArray) in omap.entrySet()) {
                relatedObjects[objType] = objIdsArray.asJsonArray.map { it.asString }
            }

            events.add(
                Event(
                    id = eventId,
                    name = name,
                    timestamp = timestamp,
                    attributes = attrs,
                    relatedObjects = relatedObjects
                )
            )
        }

        return EventLog(events = events)
    }
}
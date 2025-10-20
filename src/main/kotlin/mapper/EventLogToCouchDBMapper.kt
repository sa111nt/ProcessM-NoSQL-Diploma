package mapper

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.Gson
import db.CouchDBManager
import model.Event
import model.Trace
import model.EventLog

class EventLogToCouchDBMapper(
    private val couchDB: CouchDBManager,
    private val databaseName: String
) {
    private val gson = Gson()

    fun mapEventLogToCouchDB(eventLog: EventLog) {
        couchDB.createDatabase(databaseName)

        val eventDocs = eventLog.events.map { toJsonDoc(it) }
        val traceDocs = eventLog.traces.map { toJsonDoc(it) }

        val allDocs = eventDocs + traceDocs
        if (allDocs.isNotEmpty()) {
            val batchSize = 1000
            allDocs.chunked(batchSize).forEachIndexed { index, chunk ->
                println("📤 Uploading batch ${index + 1} of ${allDocs.size / batchSize + 1}")
                couchDB.insertBulkDocs(databaseName, chunk)
            }
        }
    }

    private fun toJsonDoc(event: Event): JsonObject {
        val doc = JsonObject()
        doc.addProperty("_id", event.id)
        doc.addProperty("type", "event")
        doc.addProperty("traceId", event.traceId ?: "")
        doc.addProperty("name", event.name ?: "")
        doc.addProperty("timestamp", event.timestamp ?: "")
        event.attributes.forEach { (k, v) -> doc.addProperty(k, v?.toString() ?: "") }
        return doc
    }

    private fun toJsonDoc(trace: Trace): JsonObject {
        val doc = JsonObject()
        doc.addProperty("_id", trace.id)
        doc.addProperty("type", "trace")

        val eventsArray = JsonArray()
        trace.events.forEach { eventsArray.add(it.id) }
        doc.add("events", eventsArray)

        trace.attributes.forEach { (k, v) -> doc.addProperty(k, v?.toString() ?: "") }
        return doc
    }
}
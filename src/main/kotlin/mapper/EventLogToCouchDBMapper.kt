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

        val logId = "log_metadata_${eventLog.importTimestamp}"
        val eventDocs = eventLog.events.map { toJsonDoc(it, logId) }
        val traceDocs = eventLog.traces.map { toJsonDoc(it, logId) }
        val logDoc = toJsonDoc(eventLog, logId)

        val allDocs = eventDocs + traceDocs + listOf(logDoc)

        if (allDocs.isNotEmpty()) {
            val batchSize = 1000
            val chunks = allDocs.chunked(batchSize)
            val totalBatches = chunks.size

            println("📦 Inserting ${allDocs.size} docs in $totalBatches batches...")

            chunks.forEachIndexed { index, chunk ->
                println("📤 Uploading batch ${index + 1} of $totalBatches (${chunk.size} documents)")
                couchDB.insertBulkDocs(databaseName, chunk)
            }
        }
    }

    private fun toJsonDoc(eventLog: EventLog, logId: String): JsonObject {
        val doc = JsonObject()
        doc.addProperty("_id", logId)
        doc.addProperty("docType", "log")
        doc.addProperty("source", eventLog.source ?: "unknown")
        doc.addProperty("importTimestamp", eventLog.importTimestamp)

        val attributesObject = JsonObject()
        eventLog.attributes.forEach { (k, v) ->
            addJsonProperty(attributesObject, k, v)
        }
        doc.add("log_attributes", attributesObject)

        return doc
    }

    private fun toJsonDoc(event: Event, logId: String): JsonObject {
        val doc = JsonObject()
        doc.addProperty("_id", event.id)
        doc.addProperty("docType", "event")
        doc.addProperty("logId", logId)
        doc.addProperty("traceId", event.traceId)
        doc.addProperty("activity", event.name)
        doc.addProperty("timestamp", event.timestamp)

        val attributesObject = JsonObject()
        event.attributes.forEach { (k, v) -> addJsonProperty(attributesObject, k, v) }
        doc.add("xes_attributes", attributesObject)

        return doc
    }

    private fun toJsonDoc(trace: Trace, logId: String): JsonObject {
        val doc = JsonObject()
        doc.addProperty("_id", trace.id)
        doc.addProperty("docType", "trace")
        doc.addProperty("logId", logId)

        val eventsArray = JsonArray()
        trace.events.forEach { eventsArray.add(it.id) }
        doc.add("eventIds", eventsArray)

        val attributesObject = JsonObject()
        trace.attributes.forEach { (k, v) -> addJsonProperty(attributesObject, k, v) }
        doc.add("xes_attributes", attributesObject)

        return doc
    }

    private fun addJsonProperty(obj: JsonObject, key: String, value: Any?) {
        when (value) {
            is String -> obj.addProperty(key, value)
            is Number -> obj.addProperty(key, value)
            is Boolean -> obj.addProperty(key, value)
            else -> obj.addProperty(key, value?.toString())
        }
    }
}
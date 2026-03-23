package mapper

import com.google.gson.stream.JsonWriter
import input.parsers.XESComponent
import input.parsers.XESTrace
import input.parsers.XESLogAttributes
import db.CouchDBManager
import model.Event
import model.Trace
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class StreamingXesToCouchDBMapper(
    private val couchDB: CouchDBManager,
    private val databaseName: String,
    private val batchSize: Int = 2500,
    private val parallelism: Int = 4
) {

    fun map(parser: Iterator<XESComponent>) {
        couchDB.createDb(databaseName)

        val logId = "log_${UUID.randomUUID()}"
        println("[INFO] Starting import for Log ID: $logId")

        val executor = ThreadPoolExecutor(
            parallelism, parallelism,
            0L, TimeUnit.MILLISECONDS,
            ArrayBlockingQueue(parallelism * 2),
            ThreadPoolExecutor.CallerRunsPolicy()
        )

        var baos = ByteArrayOutputStream()
        var writer = JsonWriter(OutputStreamWriter(baos, StandardCharsets.UTF_8))
        writer.beginObject()
        writer.name("docs")
        writer.beginArray()

        var currentDocCount = 0
        var currentLogAttr: XESLogAttributes? = null

        fun submitBatch() {
            if (currentDocCount == 0) return

            writer.endArray()
            writer.endObject()
            writer.flush()
            writer.close()

            val payload = baos.toByteArray()
            executor.submit {
                try {
                    couchDB.insertBulkDocsRaw(databaseName, payload)
                } catch (e: Exception) {
                    println("[ERROR] Błąd zapisu paczki: ${e.message}")
                }
            }

            baos = ByteArrayOutputStream()
            writer = JsonWriter(OutputStreamWriter(baos, StandardCharsets.UTF_8))
            writer.beginObject()
            writer.name("docs")
            writer.beginArray()
            currentDocCount = 0
        }

        parser.forEach { component ->
            when (component) {
                is XESLogAttributes -> {
                    currentLogAttr = component
                    writeLog(writer, component, logId)
                    currentDocCount++
                    if (currentDocCount >= batchSize) submitBatch()
                }
                is XESTrace -> {
                    writeTrace(writer, component.trace, logId, currentLogAttr)
                    currentDocCount++
                    if (currentDocCount >= batchSize) submitBatch()

                    val parentTraceId = component.trace.id.toString()

                    component.trace.events.forEachIndexed { index, event ->
                        writeEvent(writer, event, logId, index, parentTraceId, component.trace, currentLogAttr)
                        currentDocCount++
                        if (currentDocCount >= batchSize) submitBatch()
                    }
                }
            }
        }

        submitBatch()

        executor.shutdown()
        try {
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) executor.shutdownNow()
        } catch (e: InterruptedException) {
            executor.shutdownNow()
        }
        println("[INFO] Import finished.")
    }

    private fun writeLog(writer: JsonWriter, logAttr: XESLogAttributes, logId: String) {
        writer.beginObject()
        writer.name("_id").value(logId)
        writer.name("docType").value("log")
        writer.name("source").value(logAttr.attributes["source"]?.toString() ?: "unknown")
        writer.name("importTimestamp").value(System.currentTimeMillis())

        if (logAttr.attributes["identity:id"] != null) {
            writer.name("identity:id").value(logAttr.attributes["identity:id"].toString())
        }

        writer.name("log_attributes")
        writeAttributes(writer, logAttr.attributes)
        writer.endObject()
    }

    private fun writeTrace(writer: JsonWriter, trace: Trace, logId: String, logAttr: XESLogAttributes?) {
        writer.beginObject()
        writer.name("_id").value("${logId}_${trace.id}")
        writer.name("docType").value("trace")
        writer.name("logId").value(logId)
        writer.name("originalTraceId").value(trace.id.toString())
        writer.name("identity:id").value(trace.id.toString())

        val logName = logAttr?.attributes?.get("concept:name")?.toString() ?: logId
        writer.name("l:concept:name").value(logName)

        writer.name("xes_attributes")
        writeAttributes(writer, trace.attributes)
        writer.endObject()
    }

    private fun writeEvent(writer: JsonWriter, event: Event, logId: String, index: Int, traceIdStr: String, trace: Trace, logAttr: XESLogAttributes?) {
        writer.beginObject()
        writer.name("_id").value("${logId}_${event.id}")
        writer.name("docType").value("event")
        writer.name("logId").value(logId)
        writer.name("traceId").value("${logId}_$traceIdStr")

        writer.name("identity:id").value(event.id.toString())
        writer.name("activity").value(event.name)
        writer.name("timestamp").value(event.timestamp)
        writer.name("eventIndex").value(index)

        val logName = logAttr?.attributes?.get("concept:name")?.toString() ?: logId
        writer.name("l:concept:name").value(logName)

        val traceName = trace.attributes["concept:name"]?.toString() ?: trace.id.toString()
        writer.name("t:concept:name").value(traceName)

        // --- DODANE WSPARCIE DLA KLASYFIKATORÓW PQL ---
        // Generujemy "w locie" złożenia klasyfikatorów i dodajemy do JSON-a Eventu
        val classifiers = logAttr?.classifiers ?: emptyMap()
        for ((cName, cKeys) in classifiers) {
            val cValues = cKeys.mapNotNull { key ->
                event.attributes[key]?.toString()
                    ?: trace.attributes[key]?.toString()
                    ?: logAttr?.attributes?.get(key)?.toString()
            }
            if (cValues.isNotEmpty()) {
                writer.name("c:$cName").value(cValues.joinToString("+"))
            }
        }

        writer.name("xes_attributes")
        writeAttributes(writer, event.attributes)
        writer.endObject()
    }

    private fun writeAttributes(writer: JsonWriter, attributes: Map<String, Any?>) {
        writer.beginObject()
        for ((k, v) in attributes) {
            try {
                val nullableKey: String? = k as String?
                val safeKey = if (!nullableKey.isNullOrBlank()) nullableKey else "unknown_attribute_key"
                writer.name(safeKey)
                writeValue(writer, v)
            } catch (e: Exception) {
                println("[ERROR] Błąd klucza wejściowego: $k")
                throw e
            }
        }
        writer.endObject()
    }

    private fun writeValue(writer: JsonWriter, v: Any?) {
        when (v) {
            is Number -> writer.value(v)
            is Boolean -> writer.value(v)
            is String -> writer.value(v)
            is List<*> -> {
                writer.beginArray()
                v.forEach { writeValue(writer, it) }
                writer.endArray()
            }
            is Map<*, *> -> {
                writer.beginObject()
                v.forEach { (mapKey, mapVal) ->
                    writer.name(mapKey?.toString() ?: "null_key")
                    writeValue(writer, mapVal)
                }
                writer.endObject()
            }
            null -> writer.nullValue()
            else -> writer.value(v.toString())
        }
    }
}
package app

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import db.CouchDBManager
import java.io.File
import java.io.Writer

class XesExporter(private val dbManager: CouchDBManager, private val dbName: String) {

    fun exportToFile(logId: String, outputFilePath: String) {
        println("Rozpoczynam eksport logu '$logId' do pliku '$outputFilePath'...")
        val startTime = System.currentTimeMillis()

        val logQuery = """{"selector": {"docType": "log", "_id": "$logId"}, "limit": 1}"""
        val logDocs = dbManager.findDocs(dbName, com.google.gson.JsonParser.parseString(logQuery).asJsonObject)

        val tracesQuery = """{"selector": {"docType": "trace", "logId": "$logId"}, "limit": 100000}"""
        val traces = dbManager.findDocs(dbName, com.google.gson.JsonParser.parseString(tracesQuery).asJsonObject)

        val eventsQuery = """{"selector": {"docType": "event", "logId": "$logId"}, "limit": 1000000}"""
        val events = dbManager.findDocs(dbName, com.google.gson.JsonParser.parseString(eventsQuery).asJsonObject)

        val eventsByTrace = mutableMapOf<String, MutableList<JsonObject>>()
        for (i in 0 until events.size()) {
            val eventObj = events[i].asJsonObject
            val traceId = eventObj.get("traceId")?.takeIf { !it.isJsonNull }?.asString ?: continue
            eventsByTrace.computeIfAbsent(traceId) { mutableListOf() }.add(eventObj)
        }

        eventsByTrace.values.forEach { eventList ->
            eventList.sortBy { getTimestampOrEmpty(it) }
        }

        val file = File(outputFilePath)
        file.bufferedWriter().use { writer ->
            writer.write("""<?xml version="1.0" encoding="UTF-8"?>""" + "\n")
            writer.write("""<log xes.version="1.0" xmlns="http://www.xes-standard.org/">""" + "\n")
            writer.write("""  <extension name="Concept" prefix="concept" uri="http://www.xes-standard.org/concept.xesext"/>""" + "\n")
            writer.write("""  <extension name="Time" prefix="time" uri="http://www.xes-standard.org/time.xesext"/>""" + "\n")
            writer.write("""  <extension name="Organizational" prefix="org" uri="http://www.xes-standard.org/org.xesext"/>""" + "\n")
            writer.write("""  <extension name="Identity" prefix="identity" uri="http://www.xes-standard.org/identity.xesext"/>""" + "\n")

            if (logDocs.size() > 0) {
                writeAttributes(writer, logDocs[0].asJsonObject, 2)
            }

            for (i in 0 until traces.size()) {
                val traceObj = traces[i].asJsonObject
                val traceIdElem = traceObj.get("_id")
                if (traceIdElem == null || traceIdElem.isJsonNull) continue
                val traceId = traceIdElem.asString

                writer.write("  <trace>\n")
                writeAttributes(writer, traceObj, 4)

                val traceEvents = eventsByTrace[traceId] ?: emptyList()
                for (eventObj in traceEvents) {
                    writer.write("    <event>\n")
                    writeAttributes(writer, eventObj, 6)
                    writer.write("    </event>\n")
                }

                writer.write("  </trace>\n")
            }
            writer.write("</log>\n")
        }

        val duration = System.currentTimeMillis() - startTime
        println("✅ Pomyślnie wyeksportowano ${traces.size()} śladów i ${events.size()} zdarzeń w czasie ${duration}ms.")
    }

    private fun writeAttributes(writer: Writer, jsonObject: JsonObject, indentLevel: Int) {
        val indent = " ".repeat(indentLevel)

        // POPRAWKA: Eksporter obsługuje teraz klucz "attributes" wygenerowany przez nasz Mapper
        val targetObj = when {
            jsonObject.has("log_attributes") && !jsonObject.get("log_attributes").isJsonNull -> jsonObject.getAsJsonObject("log_attributes")
            jsonObject.has("attributes") && !jsonObject.get("attributes").isJsonNull -> jsonObject.getAsJsonObject("attributes")
            jsonObject.has("xes_attributes") && !jsonObject.get("xes_attributes").isJsonNull -> jsonObject.getAsJsonObject("xes_attributes")
            else -> jsonObject
        }

        val combinedAttributes = JsonObject()
        for ((k, v) in targetObj.entrySet()) {
            combinedAttributes.add(k, v)
        }

        if (jsonObject.has("identity:id") && !jsonObject.get("identity:id").isJsonNull) combinedAttributes.add("identity:id", jsonObject.get("identity:id"))
        if (jsonObject.has("timestamp") && !jsonObject.get("timestamp").isJsonNull) combinedAttributes.add("time:timestamp", jsonObject.get("timestamp"))
        if (jsonObject.has("activity") && !jsonObject.get("activity").isJsonNull) combinedAttributes.add("concept:name", jsonObject.get("activity"))
        if (jsonObject.has("source") && !jsonObject.get("source").isJsonNull) combinedAttributes.add("source", jsonObject.get("source"))

        for ((key, element) in combinedAttributes.entrySet()) {
            if (key.startsWith("_") || key == "docType" || key == "logId" || key == "traceId" || key == "eventIndex" || key == "originalTraceId") continue
            if (element.isJsonNull) continue

            val primitive = if (element.isJsonPrimitive) element.asJsonPrimitive else continue
            val valueStr = primitive.asString
            val safeValue = escapeXml(valueStr)

            val tag = when {
                key == "identity:id" -> "id"
                key == "time:timestamp" || valueStr.matches(Regex("""^\d{4}-\d{2}-\d{2}T.*""")) -> "date"
                primitive.isNumber && valueStr.contains(".") -> "float"
                primitive.isNumber -> "int"
                primitive.isBoolean -> "boolean"
                else -> {
                    try {
                        java.util.UUID.fromString(valueStr)
                        "id"
                    } catch (e: Exception) {
                        "string"
                    }
                }
            }

            writer.write("$indent<$tag key=\"$key\" value=\"$safeValue\"/>\n")
        }
    }

    private fun getTimestampOrEmpty(eventObj: JsonObject): String {
        if (eventObj.has("timestamp") && !eventObj.get("timestamp").isJsonNull) {
            return eventObj.get("timestamp").asString
        }
        if (eventObj.has("xes_attributes") && !eventObj.get("xes_attributes").isJsonNull) {
            val attrs = eventObj.getAsJsonObject("xes_attributes")
            if (attrs.has("time:timestamp") && !attrs.get("time:timestamp").isJsonNull) {
                return attrs.get("time:timestamp").asString
            }
        }
        return ""
    }

    private fun escapeXml(input: String): String {
        return input.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\"", "&quot;")
            .replace("'", "&apos;")
    }
}
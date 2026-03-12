package app

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import db.CouchDBManager
import java.io.File
import java.io.Writer

class XesExporter(private val dbManager: CouchDBManager, private val dbName: String) {

    /**
     * Eksportuje Log z bazy CouchDB z powrotem do pliku XML w standardzie XES.
     */
    fun exportToFile(logId: String, outputFilePath: String) {
        println("Rozpoczynam eksport logu '$logId' do pliku '$outputFilePath'...")
        val startTime = System.currentTimeMillis()

        // 1. Pobieranie Śladów (Traces)
        val tracesQuery = """{"selector": {"docType": "trace", "logId": "$logId"}, "limit": 100000}"""
        val traces = dbManager.findDocs(dbName, com.google.gson.JsonParser.parseString(tracesQuery).asJsonObject)

        // 2. Pobieranie Zdarzeń (Events)
        val eventsQuery = """{"selector": {"docType": "event", "logId": "$logId"}, "limit": 1000000}"""
        val events = dbManager.findDocs(dbName, com.google.gson.JsonParser.parseString(eventsQuery).asJsonObject)

        // 3. Grupowanie zdarzeń po traceId w pamięci (optymalizacja szybkości)
        val eventsByTrace = mutableMapOf<String, MutableList<JsonObject>>()
        for (i in 0 until events.size()) {
            val eventObj = events[i].asJsonObject
            val traceId = eventObj.get("traceId")?.asString ?: continue
            eventsByTrace.computeIfAbsent(traceId) { mutableListOf() }.add(eventObj)
        }

        // Sortowanie zdarzeń w ramach śladu po timestampie (jeśli istnieje)
        eventsByTrace.values.forEach { eventList ->
            eventList.sortBy { getTimestampOrEmpty(it) }
        }

        // 4. Budowanie i zapis pliku XES (strumieniowo, aby oszczędzać RAM)
        val file = File(outputFilePath)
        file.bufferedWriter().use { writer ->
            // Nagłówek pliku XES
            writer.write("""<?xml version="1.0" encoding="UTF-8"?>""" + "\n")
            writer.write("""<log xes.version="1.0" xmlns="http://www.xes-standard.org/">""" + "\n")

            // Podstawowe rozszerzenia XES (Standardowe tagi)
            writer.write("""  <extension name="Concept" prefix="concept" uri="http://www.xes-standard.org/concept.xesext"/>""" + "\n")
            writer.write("""  <extension name="Time" prefix="time" uri="http://www.xes-standard.org/time.xesext"/>""" + "\n")
            writer.write("""  <extension name="Organizational" prefix="org" uri="http://www.xes-standard.org/org.xesext"/>""" + "\n")

            // Zapisywanie Śladów i Zdarzeń
            for (i in 0 until traces.size()) {
                val traceObj = traces[i].asJsonObject
                val traceId = traceObj.get("_id").asString

                writer.write("  <trace>\n")
                // Atrybuty śladu
                writeAttributes(writer, traceObj, 4)

                // Pobranie i zapis zdarzeń dla tego śladu
                val traceEvents = eventsByTrace[traceId] ?: emptyList()
                for (eventObj in traceEvents) {
                    writer.write("    <event>\n")
                    writeAttributes(writer, eventObj, 6)
                    writer.write("    </event>\n")
                }

                writer.write("  </trace>\n")
            }

            // Zamknięcie tagu log
            writer.write("</log>\n")
        }

        val duration = System.currentTimeMillis() - startTime
        println("✅ Pomyślnie wyeksportowano ${traces.size()} śladów i ${events.size()} zdarzeń w czasie ${duration}ms.")
    }

    /**
     * Konwertuje JSON-a na tagi XES (<string>, <date>, <int>, <float>) na podstawie typu danych.
     */
    private fun writeAttributes(writer: Writer, jsonObject: JsonObject, indentLevel: Int) {
        val indent = " ".repeat(indentLevel)

        // Zależnie od tego, czy atrybuty XES w Twojej bazie są bezpośrednio w obiekcie,
        // czy w zagnieżdżonym słowniku "xes_attributes" (lub "attributes").
        // Tutaj przechodzimy po docelowych danych omijając techniczne klucze CouchDB.

        val targetObj = if (jsonObject.has("xes_attributes")) jsonObject.getAsJsonObject("xes_attributes") else jsonObject

        for ((key, element) in targetObj.entrySet()) {
            // Pomijanie kluczy technicznych bazy NoSQL
            if (key.startsWith("_") || key == "docType" || key == "logId" || key == "traceId") continue
            if (element.isJsonNull) continue

            val primitive = element.asJsonPrimitive
            val valueStr = primitive.asString
            val safeValue = escapeXml(valueStr)

            // Prosta detekcja typów XES
            val tag = when {
                primitive.isNumber && valueStr.contains(".") -> "float"
                primitive.isNumber -> "int"
                primitive.isBoolean -> "boolean"
                // Wykrywanie dat w formacie ISO8601, np. 2007-06-13T01:00:00.000+02:00
                valueStr.matches(Regex("""^\d{4}-\d{2}-\d{2}T.*""")) -> "date"
                else -> "string"
            }

            // Obsługa kluczy zagnieżdżonych (np. time:timestamp) - omijamy zagnieżdżenia, XES używa dwukropka w 'key'
            writer.write("$indent<$tag key=\"$key\" value=\"$safeValue\"/>\n")
        }
    }

    private fun getTimestampOrEmpty(eventObj: JsonObject): String {
        // Szukamy po wyciągniętym na wierzch polu 'timestamp' lub w atrybutach XES
        if (eventObj.has("timestamp")) return eventObj.get("timestamp").asString
        if (eventObj.has("xes_attributes")) {
            val attrs = eventObj.getAsJsonObject("xes_attributes")
            if (attrs.has("time:timestamp")) return attrs.get("time:timestamp").asString
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
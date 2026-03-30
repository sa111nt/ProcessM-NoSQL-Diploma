package app

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import db.CouchDBManager
import java.io.File
import java.io.Writer
import java.util.UUID

class XesExporter(private val dbManager: CouchDBManager, private val dbName: String) {

    private class XesNode(val tag: String, val key: String, var value: String? = null) {
        val children = mutableMapOf<String, XesNode>()
    }

    fun exportToFile(logId: String, outputFilePath: String) {
        val logQuery = """{"selector": {"docType": "log", "_id": "$logId"}, "limit": 1}"""
        val logDocs = dbManager.findDocs(dbName, JsonParser.parseString(logQuery).asJsonObject)

        val tracesQuery = """{"selector": {"docType": "trace", "logId": "$logId"}, "limit": 100000}"""
        val traces = dbManager.findDocs(dbName, JsonParser.parseString(tracesQuery).asJsonObject)

        val eventsQuery = """{"selector": {"docType": "event", "logId": "$logId"}, "limit": 1000000}"""
        val events = dbManager.findDocs(dbName, JsonParser.parseString(eventsQuery).asJsonObject)

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

            if (logDocs.size() > 0) {
                val logObj = logDocs[0].asJsonObject

                if (logObj.has("extensions") && logObj.get("extensions").isJsonArray) {
                    val exts = logObj.getAsJsonArray("extensions")
                    for (i in 0 until exts.size()) {
                        val ext = exts[i].asJsonObject
                        writer.write("""  <extension name="${escapeXml(ext.get("name")?.asString ?: "")}" prefix="${escapeXml(ext.get("prefix")?.asString ?: "")}" uri="${escapeXml(ext.get("uri")?.asString ?: "")}"/>""" + "\n")
                    }
                }

                if (logObj.has("globals") && logObj.get("globals").isJsonArray) {
                    val globals = logObj.getAsJsonArray("globals")
                    for (i in 0 until globals.size()) {
                        val g = globals[i].asJsonObject
                        val scope = escapeXml(g.get("scope")?.asString ?: "")
                        writer.write("  <global scope=\"$scope\">\n")
                        if (g.has("attributes")) {
                            writeAttributes(writer, g.getAsJsonObject("attributes"), 4)
                        }
                        writer.write("  </global>\n")
                    }
                }

                if (logObj.has("classifiers") && logObj.get("classifiers").isJsonArray) {
                    val classifiers = logObj.getAsJsonArray("classifiers")
                    for (i in 0 until classifiers.size()) {
                        val c = classifiers[i].asJsonObject
                        writer.write("""  <classifier name="${escapeXml(c.get("name")?.asString ?: "")}" keys="${escapeXml(c.get("keys")?.asString ?: "")}"/>""" + "\n")
                    }
                }

                writeAttributes(writer, logObj, 2)
            }

            for (i in 0 until traces.size()) {
                val traceObj = traces[i].asJsonObject
                val traceId = traceObj.get("_id")?.takeIf { !it.isJsonNull }?.asString ?: continue

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
    }

    private fun writeAttributes(writer: Writer, jsonObject: JsonObject, indentLevel: Int) {
        val targetObj = when {
            jsonObject.has("log_attributes") && !jsonObject.get("log_attributes").isJsonNull -> jsonObject.getAsJsonObject("log_attributes")
            jsonObject.has("xes_attributes") && !jsonObject.get("xes_attributes").isJsonNull -> jsonObject.getAsJsonObject("xes_attributes")
            jsonObject.has("attributes") && !jsonObject.get("attributes").isJsonNull -> jsonObject.getAsJsonObject("attributes")
            else -> jsonObject
        }

        val typesObj = if (targetObj.has("_types")) targetObj.getAsJsonObject("_types") else JsonObject()

        val combinedAttributes = JsonObject()
        targetObj.entrySet().forEach { (k, v) -> combinedAttributes.add(k, v) }

        if (jsonObject.has("identity:id") && !jsonObject.get("identity:id").isJsonNull) combinedAttributes.add("identity:id", jsonObject.get("identity:id"))
        if (jsonObject.has("timestamp") && !jsonObject.get("timestamp").isJsonNull) combinedAttributes.add("time:timestamp", jsonObject.get("timestamp"))
        if (jsonObject.has("activity") && !jsonObject.get("activity").isJsonNull) combinedAttributes.add("concept:name", jsonObject.get("activity"))

        val rootNodes = mutableMapOf<String, XesNode>()

        for ((key, element) in combinedAttributes.entrySet()) {
            if (key == "_types" || key.startsWith("_") || key == "docType" || key == "logId" || key == "traceId" || key == "eventIndex" || key == "originalTraceId") continue
            if (key == "extensions" || key == "globals" || key == "classifiers" || key == "importTimestamp" || key == "source") continue
            if (element.isJsonNull) continue

            val primitive = if (element.isJsonPrimitive) element.asJsonPrimitive else continue
            val valueStr = primitive.asString

            val explicitType = if (typesObj.has(key)) typesObj.get(key).asString else null

            if (explicitType != null && explicitType.contains("[")) {
                val parts = explicitType.split("|")
                var currentMap = rootNodes

                for (i in parts.indices) {
                    val part = parts[i]
                    val tagEnd = part.indexOf('[')
                    if (tagEnd == -1) continue

                    val tag = part.substring(0, tagEnd)
                    val partKey = part.substring(tagEnd + 1, part.length - 1)

                    val node = currentMap.getOrPut(partKey) { XesNode(tag, partKey) }

                    if (i == parts.size - 1) {
                        node.value = valueStr
                    }
                    currentMap = node.children
                }
            } else {
                val tag = explicitType ?: when {
                    primitive.isBoolean -> "boolean"
                    primitive.isNumber -> if (valueStr.contains(".") || valueStr.contains("e") || valueStr.contains("E")) "float" else "int"
                    key.contains("identity:id") || isPotentialUuid(valueStr) -> "id"
                    valueStr.matches(Regex("""^\d{4}-\d{2}-\d{2}T.*""")) -> "date"
                    else -> "string"
                }
                val node = rootNodes.getOrPut(key) { XesNode(tag, key) }
                node.value = valueStr
            }
        }

        for (node in rootNodes.values) {
            writeNode(writer, node, indentLevel)
        }
    }

    private fun writeNode(writer: Writer, node: XesNode, indentLevel: Int) {
        val indent = " ".repeat(indentLevel)
        val safeKey = escapeXml(node.key)

        if (node.children.isEmpty()) {
            val safeValue = escapeXml(node.value ?: "")
            writer.write("$indent<${node.tag} key=\"$safeKey\" value=\"$safeValue\"/>\n")
        } else {
            writer.write("$indent<${node.tag} key=\"$safeKey\">\n")
            for (child in node.children.values) {
                writeNode(writer, child, indentLevel + 2)
            }
            writer.write("$indent</${node.tag}>\n")
        }
    }

    private fun isPotentialUuid(s: String): Boolean {
        return try {
            s.length >= 32 && s.contains("-")
        } catch (e: Exception) { false }
    }

    private fun getTimestampOrEmpty(eventObj: JsonObject): String {
        return eventObj.get("timestamp")?.takeIf { !it.isJsonNull }?.asString
            ?: eventObj.getAsJsonObject("xes_attributes")?.get("time:timestamp")?.takeIf { !it.isJsonNull }?.asString
            ?: ""
    }

    private fun escapeXml(input: String): String {
        return input.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\"", "&quot;")
            .replace("'", "&apos;")
    }
}
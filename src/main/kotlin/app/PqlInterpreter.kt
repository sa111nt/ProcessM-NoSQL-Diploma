package app

import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import db.CouchDBManager
import pql.PqlParser
import pql.PqlProjection
import pql.PqlQuery
import pql.PqlToCouchDbTranslator
import java.io.BufferedReader
import java.io.InputStreamReader

class PqlInterpreter(
    private val couchDB: CouchDBManager,
    private val databaseName: String = "event_logs"
) {

    private val parser = PqlParser()
    private val translator = PqlToCouchDbTranslator()
    private val gson = GsonBuilder().setPrettyPrinting().create()

    fun start() {
        println("💬 Enter PQL queries (finish with ';', type 'exit' to quit)")
        val reader = BufferedReader(InputStreamReader(System.`in`))
        val buffer = StringBuilder()

        while (true) {
            val prompt = if (buffer.isEmpty()) "PQL> " else "... "
            print(prompt)
            val line = reader.readLine() ?: break
            val trimmed = line.trim()

            if (buffer.isEmpty() && trimmed.equals("exit", ignoreCase = true)) {
                println("👋 Exiting PQL interpreter.")
                break
            }
            if (trimmed.equals("exit", ignoreCase = true)) {
                println("⚠️ Pending query discarded.")
                buffer.clear()
                continue
            }

            buffer.appendLine(line)
            if (trimmed.endsWith(";")) {
                val statement = buffer.toString().trim()
                buffer.clear()
                val queryText = statement.removeSuffix(";").trim()
                if (queryText.isNotEmpty()) {
                    processQuery(queryText)
                }
            }
        }
    }

    private fun processQuery(queryText: String) {
        try {
            val pqlQuery = parser.parse(queryText)
            pqlQuery.orderBy?.let {
                val sortField = translator.resolveField(it.scope, it.attribute).toDotPath()
                couchDB.ensureIndex(databaseName, listOf("docType", sortField))
            }
            val couchQuery = translator.translate(pqlQuery)
            val response = couchDB.findDocs(databaseName, couchQuery)
            val output = formatResponse(response, pqlQuery)
            println(gson.toJson(output))
        } catch (e: Exception) {
            println("❌ ${e.message}")
        }
    }

    private fun formatResponse(docs: JsonArray, query: PqlQuery): JsonElement {
        if (query.selectAll || query.projections.isEmpty()) {
            return docs
        }

        val projected = JsonArray()
        for (element in docs) {
            if (!element.isJsonObject) continue
            val source = element.asJsonObject
            val projectionObject = JsonObject()
            query.projections.filterNot(PqlProjection::allAttributes).forEach { projection ->
                val scope = projection.scope ?: query.collection
                val attribute = projection.attribute ?: return@forEach
                val fieldPath = translator.resolveField(scope, attribute)
                val value = extractValue(source, fieldPath.segments)
                projectionObject.add(projection.alias ?: projection.raw, value ?: JsonNull.INSTANCE)
            }
            projected.add(projectionObject)
        }
        return projected
    }

    private fun extractValue(source: JsonObject, segments: List<String>): JsonElement? {
        var current: JsonElement = source
        for (segment in segments) {
            if (!current.isJsonObject) return null
            current = current.asJsonObject.get(segment) ?: return null
        }
        return current
    }
}


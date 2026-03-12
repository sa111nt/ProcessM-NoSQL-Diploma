package app

import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import db.CouchDBManager
import pql.parser.AntlrPqlParser
import pql.model.PqlDeleteQuery
import pql.model.PqlQuery
import pql.translator.PqlToCouchDbTranslator
import pql.executor.PqlQueryExecutor

class PqlInterpreter(
    private val dbManager: CouchDBManager,
    private val dbName: String
) {
    private val parser = AntlrPqlParser()
    private val translator = PqlToCouchDbTranslator()
    private val gson = GsonBuilder().setPrettyPrinting().create()

    // Executor do formatowanych zapytań PQL
    private val executor = PqlQueryExecutor(dbManager, dbName)

    fun executeQuery(pql: String): JsonArray {
        val parsedObject = parser.parse(pql)

        return when (parsedObject) {

            // --- OBSŁUGA DLA SELECT ---
            is PqlQuery -> {
                executor.execute(parsedObject)
            }

            // --- OBSŁUGA DLA DELETE ---
            is PqlDeleteQuery -> {
                println("\n--- [DEBUG PQL INTERPRETER] ROZPOCZYNAM PROCEDURĘ DELETE ---")
                val mangoQuery = translator.translateDelete(parsedObject)

                // Oczyszczamy zapytanie!
                mangoQuery.remove("fields")
                mangoQuery.remove("limit")
                mangoQuery.remove("skip")

                var selector = mangoQuery.getAsJsonObject("selector")
                if (selector == null) {
                    selector = JsonObject()
                    mangoQuery.add("selector", selector)
                }

                var docType = selector.get("docType")?.asString
                if (docType == null) {
                    val pqlLower = pql.lowercase()
                    docType = when {
                        pqlLower.contains("delete event") -> "event"
                        pqlLower.contains("delete trace") -> "trace"
                        pqlLower.contains("delete log") -> "log"
                        else -> ""
                    }
                    if (docType.isNotEmpty()) {
                        selector.addProperty("docType", docType)
                    }
                }

                // 🚨 NAPRAWA BŁĘDU: Zamiana _id na logId dla Event/Trace
                if (docType == "event" || docType == "trace") {
                    if (selector.has("_id")) {
                        selector.add("logId", selector.get("_id"))
                        selector.remove("_id")
                    }
                    if (selector.has("\$and")) {
                        val andArray = selector.getAsJsonArray("\$and")
                        for (i in 0 until andArray.size()) {
                            val condition = andArray[i].asJsonObject
                            if (condition.has("_id")) {
                                condition.add("logId", condition.get("_id"))
                                condition.remove("_id")
                            }
                        }
                    }
                }

                println("[DEBUG] 1. Ostateczne zapytanie Mango wysyłane do CouchDB:")
                println(gson.toJson(mangoQuery))

                var totalDeleted = 0

                // KASKADOWE USUWANIE (Najpierw sieroty/dzieci, potem rodzic)
                if (docType == "trace" || docType == "log") {
                    println("[DEBUG] 2. Rozpoczynam poszukiwanie dzieci do usunięcia kaskadowego...")
                    val targetDocs = dbManager.findDocs(dbName, mangoQuery)

                    for (i in 0 until targetDocs.size()) {
                        val parentId = targetDocs[i].asJsonObject.get("_id").asString

                        if (docType == "trace") {
                            val eventsQueryStr = """{"selector": {"docType": "event", "traceId": "$parentId"}}"""
                            val del = dbManager.deleteDocs(dbName, gson.fromJson(eventsQueryStr, JsonObject::class.java))
                            println("  -> Usunięto $del zdarzeń powiązanych ze śladem $parentId")
                            totalDeleted += del

                        } else if (docType == "log") {
                            val eventsQueryStr = """{"selector": {"docType": "event", "logId": "$parentId"}}"""
                            val tracesQueryStr = """{"selector": {"docType": "trace", "logId": "$parentId"}}"""

                            val delEv = dbManager.deleteDocs(dbName, gson.fromJson(eventsQueryStr, JsonObject::class.java))
                            val delTr = dbManager.deleteDocs(dbName, gson.fromJson(tracesQueryStr, JsonObject::class.java))
                            println("  -> Usunięto $delEv zdarzeń i $delTr śladów powiązanych z logiem $parentId")
                            totalDeleted += delEv + delTr
                        }
                    }
                }

                // FIZYCZNE USUNIĘCIE WŁAŚCIWYCH DOKUMENTÓW Z BAZY COUCHDB
                println("[DEBUG] 3. Wykonuję właściwe usuwanie docelowych dokumentów...")
                val deletedPrimary = dbManager.deleteDocs(dbName, mangoQuery)
                totalDeleted += deletedPrimary
                println("[DEBUG] -> Głównych dokumentów pomyślnie usunięto: $deletedPrimary")
                println("--- [DEBUG PQL INTERPRETER] KONIEC PROCEDURY DELETE ---\n")

                // Zwracamy wynik
                val response = JsonObject().apply {
                    addProperty("operation", "DELETE")
                    addProperty("status", "success")
                    addProperty("deleted_primary_docs", deletedPrimary)
                    addProperty("deleted_cascade_total", totalDeleted)
                }
                JsonArray().apply { add(response) }
            }

            else -> throw IllegalStateException("Nieznany typ zapytania w executeQuery: ${parsedObject::class.simpleName}")
        }
    }

    fun execute(pql: String): String {
        val parsedObject = parser.parse(pql)

        return when (parsedObject) {
            is PqlQuery -> executeSelect(parsedObject)
            is PqlDeleteQuery -> {
                val arrayResult = executeQuery(pql)
                gson.toJson(arrayResult)
            }
            else -> throw IllegalStateException("Nieznany typ zapytania: ${parsedObject::class.simpleName}")
        }
    }

    private fun executeSelect(query: PqlQuery): String {
        val formattedResult = executor.execute(query)
        return gson.toJson(formattedResult)
    }
}
package app

import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
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

    private val executor = PqlQueryExecutor(dbManager, dbName)

    fun executeQuery(pql: String): JsonArray {
        val parsedObject = parser.parse(pql)

        return when (parsedObject) {
            is PqlQuery -> {
                executor.execute(parsedObject)
            }

            is PqlDeleteQuery -> {
                val result = executor.executeDelete(parsedObject)
                JsonArray().apply { add(result) }
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
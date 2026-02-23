package app

import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import db.CouchDBManager
import pql.parser.AntlrPqlParser
import pql.model.PqlDeleteQuery
import pql.model.PqlQuery
import pql.translator.PqlToCouchDbTranslator

class PqlInterpreter(
    private val dbManager: CouchDBManager,
    private val dbName: String
) {
    private val parser = AntlrPqlParser()
    private val translator = PqlToCouchDbTranslator()
    private val gson = GsonBuilder().setPrettyPrinting().create()

    fun execute(pql: String): String {
        val parsedObject = parser.parse(pql)

        return when (parsedObject) {
            is PqlQuery -> executeSelect(parsedObject)
            is PqlDeleteQuery -> executeDelete(parsedObject)
            else -> throw IllegalStateException("Nieznany typ zapytania: ${parsedObject::class.simpleName}")
        }
    }

    private fun executeSelect(query: PqlQuery): String {
        val mangoQuery: JsonObject = translator.translate(query)

        val rawResult = dbManager.findDocs(dbName, mangoQuery)

        if (query.groupBy.isNotEmpty()) {
            return processGroupBy(rawResult, query)
        }

        return gson.toJson(rawResult)
    }

    private fun executeDelete(query: PqlDeleteQuery): String {
        val mangoSelector = translator.translateDelete(query)
        return "Operacja DELETE przyjęta (logika do zaimplementowania w CouchDBManager)"
    }

    private fun processGroupBy(rawJsonResults: JsonArray, query: PqlQuery): String {
        return "Wyniki wymagają grupowania po polach: ${query.groupBy.joinToString { it.attribute }}."
    }
}
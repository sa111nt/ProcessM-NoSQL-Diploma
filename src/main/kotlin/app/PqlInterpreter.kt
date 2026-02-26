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

    /**
     * Wykonuje zapytanie PQL i zwraca sformatowany wynik jako JsonArray.
     * Używa PqlQueryExecutor, który formatuje wynik (tylko żądane pola).
     * Przeznaczony do testów i programowego dostępu do wyników.
     */
    fun executeQuery(pql: String): JsonArray {
        val query = parser.parse(pql) as PqlQuery
        return executor.execute(query)
    }

    fun execute(pql: String): String {
        val parsedObject = parser.parse(pql)

        return when (parsedObject) {
            is PqlQuery -> executeSelect(parsedObject)
            is PqlDeleteQuery -> executeDelete(parsedObject)
            else -> throw IllegalStateException("Nieznany typ zapytania: ${parsedObject::class.simpleName}")
        }
    }

    // Używamy executora, który formatuje wynik (tylko żądane pola)
    private fun executeSelect(query: PqlQuery): String {
        if (query.groupBy.isNotEmpty()) {
            // GROUP BY — na razie stub, w przyszłości executor obsłuży agregacje
            val mangoQuery: JsonObject = translator.translate(query)
            val rawResult = dbManager.findDocs(dbName, mangoQuery)
            return processGroupBy(rawResult, query)
        }

        // Standardowe zapytanie — executor formatuje wynik
        val formattedResult = executor.execute(query)
        return gson.toJson(formattedResult)
    }

    private fun executeDelete(query: PqlDeleteQuery): String {
        val mangoSelector = translator.translateDelete(query)
        return "Operacja DELETE przyjęta (logika do zaimplementowania w CouchDBManager)"
    }

    private fun processGroupBy(rawJsonResults: JsonArray, query: PqlQuery): String {
        return "Wyniki wymagają grupowania po polach: ${query.groupBy.joinToString { it.attribute }}."
    }
}
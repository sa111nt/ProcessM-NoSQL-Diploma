package pql.executor

import com.google.gson.JsonArray
import com.google.gson.JsonObject
import db.CouchDBManager
import pql.model.PqlCondition
import pql.model.PqlQuery
import pql.translator.PqlFieldMapper
import pql.translator.PqlToCouchDbTranslator
import pql.model.PqlScope
import pql.model.SortDirection

/**
 * Koordynator wykonywania zapytań PQL.
 *
 * Odpowiada za pełny cykl:
 *   1. Tłumaczenie PqlQuery → Mango JSON (przez translator)
 *   2. Wysłanie zapytania do CouchDB (przez dbManager)
 *   3. Formatowanie surowego wyniku (przez formatter)
 */
 
class PqlQueryExecutor(
    private val dbManager: CouchDBManager,
    private val dbName: String
) {
    // Translator: PqlQuery → Mango JSON
    private val translator = PqlToCouchDbTranslator()

    // Mapper: nazwy pól PQL → ścieżki CouchDB
    private val fieldMapper = PqlFieldMapper()

    // Formatter: surowe dokumenty → czysty wynik PQL
    private val formatter = PqlResultFormatter(fieldMapper)

    /**
     * Wykonuje zapytanie PQL i zwraca sformatowany wynik.
     *
     * @param query Sparsowane zapytanie PQL
     * @return JsonArray z dokumentami zawierającymi tylko żądane pola
     */
    fun execute(query: PqlQuery): JsonArray {
        // Sprawdź warunki AlwaysFalse (np. WHERE 0=1) — natychmiast zwróć pusty wynik
        if (query.conditions.any { it is PqlCondition.AlwaysFalse }) {
            return JsonArray()
        }

        // Czy potrzebujemy sortowania w pamięci (cross-scope)?
        // Np. zapytanie o EVENTY posortowane po atrybucie TRACE (t:total)
        val hasCrossScopeOrderBy = query.orderBy.any { it.scope != query.collection }

        // 1. Tłumaczymy PqlQuery na zapytanie Mango (JSON dla CouchDB)
        val mangoQuery: JsonObject = translator.translate(query)

        // Jeśli sortujemy w pamięci, zdejmujemy z Mango `limit` i `skip`, bo musimy pobrać wszystko,
        // posortować lokalnie, i dopiero wtedy uciąć. Zdejmujemy też `sort`.
        if (hasCrossScopeOrderBy) {
            mangoQuery.remove("limit")
            mangoQuery.remove("skip")
            mangoQuery.remove("sort")
        }

        // 2. Jeśli jest tylko zwykły ORDER BY, tworzymy indeks
        if (query.orderBy.isNotEmpty() && !hasCrossScopeOrderBy) {
            val indexFields = mutableListOf("docType")
            for (order in query.orderBy) {
                indexFields.add(fieldMapper.resolve(order.scope, order.attribute).toDotPath())
            }
            dbManager.ensureIndex(dbName, indexFields)
        }

        // 3. Wysyłamy zapytanie do CouchDB i odbieramy surowe dokumenty
        var rawResult: JsonArray = dbManager.findDocs(dbName, mangoQuery)

        // 4. Sortowanie w pamięci (jeśli wymagane)
        if (hasCrossScopeOrderBy && rawResult.size() > 0) {
            rawResult = executeCrossScopeOrderBy(rawResult, query)
        }

        // 5. Formatujemy wynik — zostawiamy tylko te pola, które żądano w SELECT
        return formatter.format(rawResult, query)
    }

    /**
     * W CouchDB dokumenty EVENT nie mają zdenormalizowanych svih pól TRACE (np. t:total).
     * Jeśli zapytanie żąda sortowania po parent-scopie, musimy pobrać te dokumenty i
     * posortować listę w Kotlinie.
     */
    private fun executeCrossScopeOrderBy(rawResult: JsonArray, query: PqlQuery): JsonArray {
        // 1. Zbieramy identyfikatory rodziców (traceId, logId) potrzebnych do sortowania
        val traceIds = mutableSetOf<String>()
        val logIds = mutableSetOf<String>()

        val needsTrace = query.orderBy.any { it.scope == PqlScope.TRACE }
        val needsLog = query.orderBy.any { it.scope == PqlScope.LOG }

        if (needsTrace || needsLog) {
            rawResult.forEach { doc ->
                val obj = doc.asJsonObject
                if (needsTrace) obj.get("traceId")?.asString?.let { traceIds.add(it) }
                if (needsLog) obj.get("logId")?.asString?.let { logIds.add(it) }
            }
        }

        // 2. Pobieramy dokumenty nadrzędne
        val parentsMap = mutableMapOf<String, JsonObject>()

        if (traceIds.isNotEmpty()) {
            val traceQuery = JsonObject().apply {
                add("selector", JsonObject().apply {
                    add("_id", JsonObject().apply {
                        val inArray = JsonArray()
                        traceIds.forEach { inArray.add(it) }
                        add("\$in", inArray)
                    })
                })
                addProperty("limit", traceIds.size)
            }
            dbManager.findDocs(dbName, traceQuery).forEach { doc ->
                val obj = doc.asJsonObject
                parentsMap[obj.get("_id").asString] = obj
            }
        }

        if (logIds.isNotEmpty()) {
            val logQuery = JsonObject().apply {
                add("selector", JsonObject().apply {
                    add("_id", JsonObject().apply {
                        val inArray = JsonArray()
                        logIds.forEach { inArray.add(it) }
                        add("\$in", inArray)
                    })
                })
                addProperty("limit", logIds.size)
            }
            dbManager.findDocs(dbName, logQuery).forEach { doc ->
                val obj = doc.asJsonObject
                parentsMap[obj.get("_id").asString] = obj
            }
        }

        // 3. Sortujemy listę
        val sortedList = rawResult.map { it.asJsonObject }.sortedWith(Comparator { a, b ->
            var cmp = 0
            for (order in query.orderBy) {
                // Znajdź dokument docelowy: oryginalny (jeśli ten sam scope) lub dociągnięty (parent)
                val docA = if (order.scope == query.collection) a else {
                    val parentId = if (order.scope == PqlScope.TRACE) a.get("traceId")?.asString else a.get("logId")?.asString
                    parentId?.let { parentsMap[it] } ?: a
                }
                val docB = if (order.scope == query.collection) b else {
                    val parentId = if (order.scope == PqlScope.TRACE) b.get("traceId")?.asString else b.get("logId")?.asString
                    parentId?.let { parentsMap[it] } ?: b
                }

                val valA = getFieldValue(docA, fieldMapper.resolve(order.scope, order.attribute).toDotPath())
                val valB = getFieldValue(docB, fieldMapper.resolve(order.scope, order.attribute).toDotPath())

                cmp = compareValues(valA, valB)
                if (order.direction == SortDirection.DESC) {
                    cmp = -cmp
                }
                if (cmp != 0) break
            }
            cmp
        })

        // 4. Aplikujemy LIMIT i OFFSET w pamięci, bo CouchDB tego nie mogło zrobić przed sortowaniem
        var stream = sortedList.asSequence()
        query.offset?.let { stream = stream.drop(it) }
        query.limit?.let { stream = stream.take(it) }

        val finalResult = JsonArray()
        stream.forEach { finalResult.add(it) }
        return finalResult
    }

    private fun getFieldValue(doc: JsonObject, path: String): Comparable<Any>? {
        val parts = path.split(".")
        var current: com.google.gson.JsonElement? = doc
        for (part in parts) {
            if (current == null || !current.isJsonObject) return null
            current = current.asJsonObject.get(part)
        }
        if (current == null || current.isJsonNull) return null
        
        return when {
            current.isJsonPrimitive -> {
                val prim = current.asJsonPrimitive
                when {
                    prim.isNumber -> prim.asDouble as Comparable<Any> // Sortuj liczby jako double
                    prim.isBoolean -> prim.asBoolean as Comparable<Any>
                    else -> prim.asString as Comparable<Any>
                }
            }
            else -> current.toString() as Comparable<Any>
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun compareValues(a: Comparable<Any>?, b: Comparable<Any>?): Int {
        if (a == null && b == null) return 0
        if (a == null) return -1
        if (b == null) return 1
        return try {
            a.compareTo(b)
        } catch (e: Exception) {
            // Jeśli typy są różne (np. String i Double), porównujemy tekstowo
            a.toString().compareTo(b.toString())
        }
    }
}

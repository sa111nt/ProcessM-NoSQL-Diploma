package db

import com.google.gson.Gson
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.JsonParser
import okhttp3.*
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import java.io.IOException
import java.util.concurrent.TimeUnit

open class CouchDBManager(
    private val url: String = "http://127.0.0.1:5984",
    private val user: String = "admin",
    private val password: String = "admin"
) {
    private val maxConcurrency = 10
    private val gson = Gson()
    private val JSON_MEDIA_TYPE = "application/json; charset=utf-8".toMediaType()

    private val authHeader = Credentials.basic(user, password)
    private val ensuredIndexes = mutableSetOf<String>()

    private val client = OkHttpClient.Builder()
        .dispatcher(Dispatcher().apply {
            maxRequests = maxConcurrency
            maxRequestsPerHost = maxConcurrency
        })
        .connectionPool(ConnectionPool(maxConcurrency, 5, TimeUnit.MINUTES))
        .connectTimeout(60, TimeUnit.SECONDS)
        .writeTimeout(120, TimeUnit.SECONDS)
        .readTimeout(120, TimeUnit.SECONDS)
        .build()

    open fun createDb(dbName: String) {
        val emptyBody = "".toRequestBody(null)
        val request = Request.Builder()
            .put(emptyBody)
            .url("$url/$dbName")
            .header("Authorization", authHeader)
            .build()

        try {
            client.newCall(request).execute().use { response ->
                if (!response.isSuccessful && response.code != 412) {
                    throw IOException("Błąd tworzenia bazy '$dbName'. Kod: ${response.code}")
                }
            }
        } catch (e: Exception) {
            throw IOException("Nie udało się połączyć z CouchDB ($url).", e)
        }
    }

    open fun deleteDb(dbName: String) {
        val request = Request.Builder()
            .delete()
            .url("$url/$dbName")
            .header("Authorization", authHeader)
            .build()

        try {
            client.newCall(request).execute().use { response ->
                if (!response.isSuccessful && response.code != 404) {
                    throw IOException("Błąd usuwania bazy '$dbName'. Kod: ${response.code}")
                }
            }
        } catch (e: Exception) {
            throw IOException("Nie udało się połączyć podczas próby usunięcia bazy.", e)
        }
    }

    open fun insertBulkDocsRaw(dbName: String, jsonBytes: ByteArray) {
        val requestBody = jsonBytes.toRequestBody(JSON_MEDIA_TYPE)
        val request = Request.Builder()
            .post(requestBody)
            .url("$url/$dbName/_bulk_docs")
            .header("Authorization", authHeader)
            .build()

        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) throw IOException("Błąd zapisu batcha: ${response.code}")
        }
    }

    // Klasyczne pobieranie (zwraca JsonArray - używać tylko dla małych wyników)
    open fun findDocs(dbName: String, mangoQuery: JsonObject): JsonArray {
        val result = JsonArray()
        findDocsStream(dbName, mangoQuery) { doc -> result.add(doc) }
        return result
    }

    // 💡 ZŁOTA OPTYMALIZACJA OOM: Strumieniowe czytanie i parsowanie w locie! (Bez budowania wielkich tekstów)
    open fun findDocsStream(dbName: String, mangoQuery: JsonObject, onDocParsed: (JsonObject) -> Unit) {
        val requestBody = gson.toJson(mangoQuery).toRequestBody(JSON_MEDIA_TYPE)
        val request = Request.Builder()
            .post(requestBody)
            .url("$url/$dbName/_find")
            .header("Authorization", authHeader)
            .build()

        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) throw IOException("Błąd zapytania _find: ${response.code}")

            val reader = com.google.gson.stream.JsonReader(response.body!!.charStream())
            try {
                reader.beginObject()
                while (reader.hasNext()) {
                    if (reader.nextName() == "docs") {
                        reader.beginArray()
                        while (reader.hasNext()) {
                            val doc = JsonParser.parseReader(reader).asJsonObject
                            onDocParsed(doc)
                        }
                        reader.endArray()
                    } else {
                        reader.skipValue()
                    }
                }
                reader.endObject()
            } catch (e: Exception) {
                println("[ERROR] Błąd strumieniowania JSON z bazy: ${e.message}")
            }
        }
    }

    open fun ensureIndex(dbName: String, fields: List<String>) {
        if (fields.isEmpty()) return
        val signature = "$dbName:${fields.joinToString(",")}"
        if (!ensuredIndexes.add(signature)) return

        val indexDef = JsonObject().apply {
            add("index", JsonObject().apply {
                val fieldsArray = JsonArray()
                fields.forEach { fieldsArray.add(it) }
                add("fields", fieldsArray)
            })
            addProperty("type", "json")
            addProperty("name", "idx_${fields.joinToString("_")}")
        }

        val body = gson.toJson(indexDef).toRequestBody(JSON_MEDIA_TYPE)
        val request = Request.Builder()
            .post(body)
            .url("$url/$dbName/_index")
            .header("Authorization", authHeader)
            .build()

        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful && response.code != 409) {
                println("⚠️ Ostrzeżenie: Nie udało się utworzyć indeksu $fields: ${response.code}")
            }
        }
    }

    open fun deleteDocs(dbName: String, query: JsonObject): Int {
        if (!query.has("fields")) {
            query.add("fields", JsonArray().apply { add("_id"); add("_rev") })
        }
        if (!query.has("limit")) query.addProperty("limit", 100000)

        val docsToDelete = findDocs(dbName, query)
        if (docsToDelete.isEmpty) return 0

        val bulkArray = JsonArray()
        docsToDelete.forEach { doc ->
            val docObj = doc.asJsonObject
            val deleteItem = JsonObject().apply {
                add("_id", docObj.get("_id"))
                add("_rev", docObj.get("_rev"))
                addProperty("_deleted", true)
            }
            bulkArray.add(deleteItem)
        }

        val bulkBodyJson = JsonObject().apply { add("docs", bulkArray) }
        val requestBody = gson.toJson(bulkBodyJson).toRequestBody(JSON_MEDIA_TYPE)

        val request = Request.Builder()
            .post(requestBody)
            .url("$url/$dbName/_bulk_docs")
            .header("Authorization", authHeader)
            .build()

        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) error("❌ Błąd usuwania: ${response.code}")
            val reader = response.body?.charStream() ?: return 0
            val respArray = JsonParser.parseReader(reader).asJsonArray
            return respArray.count { !it.asJsonObject.has("error") }
        }
    }

    open fun close() {
        client.dispatcher.executorService.shutdown()
        client.connectionPool.evictAll()
    }
}
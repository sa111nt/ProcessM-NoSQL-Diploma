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
    // 1. Czysty URL bez loginu/hasła
    private val url: String = "http://127.0.0.1:5984",
    private val user: String = "admin",
    // 2. POPRAWKA: Hasło zgodne z docker-compose.yml
    private val password: String = "admin"
) {
    private val maxConcurrency = 10
    private val gson = Gson()
    private val JSON_MEDIA_TYPE = "application/json; charset=utf-8".toMediaType()

    // 3. Generujemy nagłówek Basic Auth raz
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

    /**
     * Tworzy nową bazę danych.
     */
    open fun createDb(dbName: String) {
        val emptyBody = "".toRequestBody(null)
        val request = Request.Builder()
            .put(emptyBody)
            .url("$url/$dbName")
            .header("Authorization", authHeader) // Zawsze dodajemy nagłówek
            .build()

        try {
            client.newCall(request).execute().use { response ->
                // 412 = Precondition Failed (Baza już istnieje) - to nie jest błąd
                if (!response.isSuccessful && response.code != 412) {
                    throw IOException("Błąd tworzenia bazy '$dbName'. Kod: ${response.code} (${response.message})")
                }
            }
        } catch (e: Exception) {
            throw IOException("Nie udało się połączyć z CouchDB ($url). Sprawdź czy kontener działa.", e)
        }
    }

    /**
     * Usuwa bazę danych o podanej nazwie.
     */
    open fun deleteDb(dbName: String) {
        val request = Request.Builder()
            .delete() // Używamy metody HTTP DELETE
            .url("$url/$dbName")
            .header("Authorization", authHeader) // Wymagana autoryzacja
            .build()

        try {
            client.newCall(request).execute().use { response ->
                // Jeśli kod to 404 (Not Found), to znaczy, że bazy i tak już nie było,
                // więc cel (brak bazy) został osiągnięty - nie rzucamy błędu.
                if (!response.isSuccessful && response.code != 404) {
                    throw IOException("Błąd usuwania bazy '$dbName'. Kod: ${response.code} (${response.message})")
                }
            }
        } catch (e: Exception) {
            throw IOException("Nie udało się połączyć z CouchDB ($url) podczas próby usunięcia bazy.", e)
        }
    }

    /**
     * Wstawia dokumenty w trybie Batch.
     */
    open fun insertBulkDocsRaw(dbName: String, jsonBytes: ByteArray) {
        val requestBody = jsonBytes.toRequestBody(JSON_MEDIA_TYPE)

        val request = Request.Builder()
            .post(requestBody)
            .url("$url/$dbName/_bulk_docs")
            .header("Authorization", authHeader)
            .build()

        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                throw IOException("Błąd zapisu batcha: ${response.code} ${response.message}")
            }
        }
    }

    /**
     * Wykonuje zapytanie Mango (JSON selector).
     */
    open fun findDocs(dbName: String, mangoQuery: JsonObject): JsonArray {
        val requestBody = gson.toJson(mangoQuery).toRequestBody(JSON_MEDIA_TYPE)

        val request = Request.Builder()
            .post(requestBody)
            .url("$url/$dbName/_find")
            .header("Authorization", authHeader)
            .build()

        client.newCall(request).execute().use { response ->
            if (!response.isSuccessful) {
                throw IOException("Błąd zapytania _find: ${response.code} ${response.message}")
            }

            val responseString = response.body?.string() ?: "{}"
            val root = JsonParser.parseString(responseString).asJsonObject

            return if (root.has("docs")) {
                root.getAsJsonArray("docs")
            } else {
                JsonArray()
            }
        }
    }

    /**
     * Tworzy indeks dla przyspieszenia zapytań.
     */
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

    /**
     * Usuwa dokumenty pasujące do zapytania.
     */
    open fun deleteDocs(dbName: String, query: JsonObject): Int {
        if (!query.has("fields")) {
            query.add("fields", JsonArray().apply {
                add("_id")
                add("_rev")
            })
        }
        if (!query.has("limit")) {
            query.addProperty("limit", 100000)
        }

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

            val respString = response.body?.string() ?: "[]"
            val respArray = JsonParser.parseString(respString).asJsonArray
            return respArray.count { !it.asJsonObject.has("error") }
        }
    }

    open fun close() {
        client.dispatcher.executorService.shutdown()
        client.connectionPool.evictAll()
    }
}
package db

import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import okhttp3.Credentials
import okhttp3.MediaType.Companion.toMediaTypeOrNull
import okhttp3.RequestBody.Companion.toRequestBody
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import com.google.gson.Gson
import java.util.concurrent.TimeUnit

class CouchDBManager(
    private val url: String = "http://localhost:5984",
    private val user: String = "admin",
    private val password: String = "admin"
) {
    // ✅ dłuższe timeouty - zapobiega SocketTimeoutException
    private val client = OkHttpClient.Builder()
        .connectTimeout(60, TimeUnit.SECONDS)
        .writeTimeout(120, TimeUnit.SECONDS)
        .readTimeout(120, TimeUnit.SECONDS)
        .build()

    private val gson = Gson()
    private val auth = Credentials.basic(user, password)

    fun createDatabase(dbName: String) {
        val emptyBody = ByteArray(0).toRequestBody(null, 0, 0)
        val request = Request.Builder()
            .put(emptyBody)
            .url("$url/$dbName")
            .header("Authorization", auth)
            .build()

        client.newCall(request).execute().use { response ->
            when {
                response.isSuccessful -> println("✅ Database created: $dbName")
                response.code == 412 -> println("ℹ️ Database '$dbName' already exists")
                else -> error("❌ Error (${response.code}): ${response.message}")
            }
        }
    }

    // ✅ batch insert — zamiast wysyłać wszystko naraz
    fun insertBulkDocs(dbName: String, docs: List<JsonObject>, batchSize: Int = 1000) {
        if (docs.isEmpty()) return

        val chunks = docs.chunked(batchSize)
        println("📦 Inserting ${docs.size} docs in ${chunks.size} batches...")

        chunks.forEachIndexed { index, chunk ->
            val bulkBody = JsonObject().apply {
                add("docs", JsonArray().apply { chunk.forEach { add(it) } })
            }

            val body: RequestBody = gson.toJson(bulkBody)
                .toRequestBody("application/json".toMediaTypeOrNull())

            val request = Request.Builder()
                .post(body)
                .url("$url/$dbName/_bulk_docs")
                .header("Authorization", auth)
                .build()

            client.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    error("❌ Error inserting batch ${index + 1}: ${response.code} ${response.message}")
                } else {
                    println("✅ Inserted batch ${index + 1}/${chunks.size} (${chunk.size} docs)")
                }
            }
        }
    }
}
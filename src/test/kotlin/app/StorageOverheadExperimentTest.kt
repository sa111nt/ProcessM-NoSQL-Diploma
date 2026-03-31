package pql

import app.LogImporter
import db.CouchDBManager
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Base64
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StorageOverheadExperimentTest {

    private val dbUrl = "http://127.0.0.1:5984"
    private val dbUser = "admin"
    private val dbPass = "admin"

    private val hospitalFile = "src/test/resources/Hospital_log.xes.gz"
    private val journalFile = "src/test/resources/JournalReview-extra.xes"

    @Test
    fun `measure storage overhead of event denormalization`() {
        val dbNormalized = "experiment_db_normalized"
        val dbDenormalized = "experiment_db_denormalized"

        val dbManager = CouchDBManager(dbUrl, dbUser, dbPass)

        println("\n=== ROZPOCZĘCIE EKSPERYMENTU PAMIĘCIOWEGO ===")

        try { dbManager.deleteDb(dbNormalized) } catch (e: Exception) {}
        try { dbManager.createDb(dbNormalized) } catch (e: Exception) {}

        LogImporter.import(hospitalFile, dbNormalized, dbManager, denormalizeEvents = false)
        LogImporter.import(journalFile, dbNormalized, dbManager, denormalizeEvents = false)

        try { dbManager.deleteDb(dbDenormalized) } catch (e: Exception) {}
        try { dbManager.createDb(dbDenormalized) } catch (e: Exception) {}

        LogImporter.import(hospitalFile, dbDenormalized, dbManager, denormalizeEvents = true)
        LogImporter.import(journalFile, dbDenormalized, dbManager, denormalizeEvents = true)

        val sizeNormalized = getDatabaseActiveSize(dbNormalized)
        val sizeDenormalized = getDatabaseActiveSize(dbDenormalized)

        val overheadBytes = sizeDenormalized - sizeNormalized
        val overheadPercent = (overheadBytes.toDouble() / sizeNormalized.toDouble()) * 100

        println("\n================ WYNIKI EKSPERYMENTU ================")
        println("Rozmiar bez denormalizacji : ${formatSize(sizeNormalized)}")
        println("Rozmiar z denormalizacją   : ${formatSize(sizeDenormalized)}")
        println("Narzut przestrzeni (Koszt) : ${formatSize(overheadBytes)}")
        println(String.format("Procentowy wzrost rozmiaru : %.2f %%", overheadPercent))
        println("=====================================================\n")

        try { dbManager.deleteDb(dbNormalized) } catch (e: Exception) {}
        try { dbManager.deleteDb(dbDenormalized) } catch (e: Exception) {}

        assertTrue(sizeDenormalized > sizeNormalized, "Zdenormalizowana baza powinna ważyć więcej z powodu powielonych danych.")
    }

    private fun getDatabaseActiveSize(dbName: String): Long {
        val authString = "$dbUser:$dbPass"
        val authHeader = "Basic " + Base64.getEncoder().encodeToString(authString.toByteArray())

        val request = HttpRequest.newBuilder()
            .uri(URI.create("$dbUrl/$dbName"))
            .header("Authorization", authHeader)
            .GET()
            .build()

        val response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())

        val activeSizeRegex = "\"active\":\\s*(\\d+)".toRegex()
        val match = activeSizeRegex.find(response.body())

        return match?.groupValues?.get(1)?.toLong() ?: throw RuntimeException("Nie udało się pobrać rozmiaru bazy: $dbName. Odpowiedź serwera: ${response.body()}")
    }

    private fun formatSize(bytes: Long): String {
        return "${bytes / 1024 / 1024} MB (${bytes / 1024} KB)"
    }
}
package app

import com.google.gson.JsonObject
import db.CouchDBManager
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.File

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class XesExporterTest {

    private lateinit var dbManager: CouchDBManager
    private lateinit var interpreter: PqlInterpreter
    private lateinit var exporter: XesExporter
    private val dbName = "event_logs_export_test"
    private lateinit var simpleLogId: String

    private val inputFilePath = "src/test/resources/simple_test.xes"
    private val outputFilePath = "src/test/resources/exported_simple_test.xes"

    @BeforeAll
    fun setup() {
        dbManager = CouchDBManager("http://127.0.0.1:5984", "admin", "admin")

        // 1. Czyszczenie i przygotowanie bazy testowej
        try { dbManager.deleteDb(dbName) } catch (e: Exception) {}
        try { dbManager.createDb(dbName) } catch (e: Exception) {}

        // 2. Importowanie naszego małego pliku testowego
        println("=== SETUP: Importowanie $inputFilePath ===")
        LogImporter.import(inputFilePath, dbName, dbManager)

        // 3. Pobranie ID zaimportowanego logu
        val logQuery = JsonObject().apply {
            add("selector", JsonObject().apply { addProperty("docType", "log") })
        }
        val logs = dbManager.findDocs(dbName, logQuery)
        assertTrue(logs.size() > 0, "Log powinien zostać poprawnie zaimportowany")

        simpleLogId = logs[0].asJsonObject.get("_id").asString
        println("Zaimportowano log z ID: $simpleLogId")

        // 4. Inicjalizacja narzędzi
        interpreter = PqlInterpreter(dbManager, dbName)
        exporter = XesExporter(dbManager, dbName)
    }

    @Test
    fun testFullDataCycleWithDeleteAndExport() {
        // --- ETAP 1: Weryfikacja stanu przed usunięciem ---
        val beforeQuery = "select e:concept:name where l:id='$simpleLogId'"
        val eventsBefore = interpreter.executeQuery(beforeQuery).asJsonArray
        assertEquals(4, eventsBefore.size(), "Przed operacją DELETE powinny być 4 zdarzenia w bazie.")

        // --- ETAP 2: Operacja DELETE (Usuwamy zdarzenia przypisane do Pete'a) ---
        val deleteQuery = "delete event where l:id='$simpleLogId' and e:org:resource = 'Pete'"
        println("\nWykonuję zapytanie: $deleteQuery")
        val deleteResult = interpreter.executeQuery(deleteQuery).asJsonArray
        println("Wynik usunięcia: $deleteResult")

        // --- ETAP 3: Weryfikacja stanu bazy po usunięciu ---
        val afterQuery = "select e:concept:name where l:id='$simpleLogId'"
        val eventsAfter = interpreter.executeQuery(afterQuery).asJsonArray
        assertEquals(3, eventsAfter.size(), "Po usunięciu Pete'a, w bazie powinny zostać 3 zdarzenia.")

        // --- ETAP 4: EKSPORT DO NOWEGO PLIKU XES ---
        println("\nRozpoczynam eksport logu do pliku: $outputFilePath")
        exporter.exportToFile(simpleLogId, outputFilePath)

        // --- ETAP 5: Weryfikacja fizycznego pliku XES ---
        val exportedFile = File(outputFilePath)
        assertTrue(exportedFile.exists(), "Wyeksportowany plik powinien istnieć na dysku!")

        // Czytamy zawartość pliku jako tekst
        val xmlContent = exportedFile.readText()

        println("\n=== SPRAWDZANIE ZAWARTOŚCI WYEKSPORTOWANEGO PLIKU ===")

        // Asercja 1: Upewniamy się, że struktura XES jest zachowana
        assertTrue(xmlContent.contains("<log"), "Plik musi zawierać tag <log>")
        assertTrue(xmlContent.contains("<trace>"), "Plik musi zawierać tag <trace>")
        assertTrue(xmlContent.contains("<event>"), "Plik musi zawierać tag <event>")

        // Asercja 2: Sprawdzamy czy Mike i Anne wciąż tam są
        assertTrue(xmlContent.contains("value=\"Mike\""), "Zdarzenia Mike'a powinny przetrwać eksport.")
        assertTrue(xmlContent.contains("value=\"Anne\""), "Zdarzenia Anne powinny przetrwać eksport.")

        // Asercja 3: Sprawdzamy, czy usunięcie zadziałało (Pete nie ma prawa być w pliku!)
        assertFalse(xmlContent.contains("value=\"Pete\""), "BŁĄD: Zdarzenie Pete'a wyciekło do pliku wyjściowego!")

        println("✅ Pełen cykl ETL (Extract, Transform, Load) przetestowany z sukcesem!")
    }
}
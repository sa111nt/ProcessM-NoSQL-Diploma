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

    private val inputFilePath = "src/test/resources/generated_complex_test.xes"
    private val outputIdentityPath = "src/test/resources/exported_identity_test.xes"
    private val outputDeletePath = "src/test/resources/exported_delete_test.xes"

    @BeforeAll
    fun setup() {
        dbManager = CouchDBManager("http://127.0.0.1:5984", "admin", "admin")
        try { dbManager.deleteDb(dbName) } catch (e: Exception) {}
        try { dbManager.createDb(dbName) } catch (e: Exception) {}

        interpreter = PqlInterpreter(dbManager, dbName)
        exporter = XesExporter(dbManager, dbName)

        File("src/test/resources").mkdirs()
    }

    /**
     * Generuje w locie złożony plik XES używany do testów, zawierający atrybuty
     * Logu, różnych typów zmienne (float, boolean, date) oraz wiele śladów.
     */
    private fun createComplexXesFile() {
        val xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <log xes.version="1.0" xmlns="http://www.xes-standard.org/">
              <extension name="Concept" prefix="concept" uri="http://www.xes-standard.org/concept.xesext"/>
              <extension name="Time" prefix="time" uri="http://www.xes-standard.org/time.xesext"/>
              <string key="concept:name" value="Bank Transfer Process"/>
              <string key="system_version" value="2.5.1"/>
              <trace>
                <string key="concept:name" value="Trace_A_Normal"/>
                <event>
                  <string key="concept:name" value="Request Received"/>
                  <date key="time:timestamp" value="2023-10-01T10:00:00.000+00:00"/>
                  <string key="org:resource" value="System"/>
                </event>
                <event>
                  <string key="concept:name" value="Funds Deducted"/>
                  <float key="amount" value="1500.50"/>
                  <boolean key="is_fraud" value="false"/>
                </event>
              </trace>
              <trace>
                <string key="concept:name" value="Trace_B_Fraud"/>
                <event>
                  <string key="concept:name" value="Request Received"/>
                  <date key="time:timestamp" value="2023-10-02T11:00:00.000+00:00"/>
                </event>
                <event>
                  <string key="concept:name" value="Block Account"/>
                  <float key="amount" value="99999.99"/>
                  <boolean key="is_fraud" value="true"/>
                  <string key="reason" value="Suspicious IP"/>
                </event>
              </trace>
            </log>
        """.trimIndent()
        File(inputFilePath).writeText(xml)
    }

    private fun getLogIdFromDb(): String {
        val logQuery = JsonObject().apply {
            add("selector", JsonObject().apply { addProperty("docType", "log") })
        }
        val logs = dbManager.findDocs(dbName, logQuery)
        assertTrue(logs.size() > 0, "Log musi zostać poprawnie zaimportowany")
        return logs[0].asJsonObject.get("_id").asString
    }

    @Test
    fun testIdentityExport() {
        createComplexXesFile()
        LogImporter.import(inputFilePath, dbName, dbManager)
        val logId = getLogIdFromDb()

        // 1. Natychmiastowy eksport bez wprowadzania zmian
        exporter.exportToFile(logId, outputIdentityPath)
        val exportedFile = File(outputIdentityPath)
        assertTrue(exportedFile.exists())

        val xmlContent = exportedFile.readText()

        // 2. Asercje Strukturalne (Sprawdzanie utraty danych)
        // Czy atrybuty Logu przetrwały?
        assertTrue(xmlContent.contains("Bank Transfer Process"), "Zgubiono nazwę logu")
        assertTrue(xmlContent.contains("key=\"system_version\""), "Zgubiono niestandardowy atrybut logu")

        // Czy Ślady i ich Zdarzenia przetrwały?
        assertTrue(xmlContent.contains("Trace_A_Normal") && xmlContent.contains("Trace_B_Fraud"), "Zgubiono nazwy śladów")
        assertTrue(xmlContent.contains("Funds Deducted") && xmlContent.contains("Block Account"), "Zgubiono nazwy zdarzeń")

        // Czy Semantic Casting działa? (Czy poprawne są tagi XML dla odpowiednich typów NoSQL)
        assertTrue(xmlContent.contains("<float"), "System zgubił typ 'float' podczas zapisu do NoSQL lub eksportu")
        assertTrue(xmlContent.contains("1500.5"), "Zgubiono wartość float 1500.5")
        assertTrue(xmlContent.contains("<boolean"), "System zgubił typ 'boolean'")
        assertTrue(xmlContent.contains("value=\"true\"") && xmlContent.contains("value=\"false\""), "Zgubiono wartości boolean")
        assertTrue(xmlContent.contains("<date"), "System zgubił typ 'date'")

        println("✅ Identity Test (Wejście = Wyjście) przeszedł pomyślnie. Wszystkie typy i hierarchie zachowane.")

        // Czyszczenie
        try { dbManager.deleteDb(dbName); dbManager.createDb(dbName) } catch (e: Exception) {}
    }

    @Test
    fun testExportAfterPqlDelete() {
        createComplexXesFile()
        LogImporter.import(inputFilePath, dbName, dbManager)
        val logId = getLogIdFromDb()

        // 1. Wykonanie zapytania PQL modyfikującego bazę danych
        // Usuwamy tylko i wyłącznie oszukańcze zdarzenia (gdzie amount > 5000)
        val deleteQuery = "delete event where l:id='$logId' and e:amount > 5000"
        val deleteResult = interpreter.executeQuery(deleteQuery).asJsonArray
        assertTrue(deleteResult[0].asJsonObject.get("deleted_primary_docs").asInt > 0, "Procedura DELETE powinna usunąć oszukańcze zdarzenie")

        // 2. Eksportowanie bazy po modyfikacji
        exporter.exportToFile(logId, outputDeletePath)
        val exportedFile = File(outputDeletePath)
        assertTrue(exportedFile.exists())

        val xmlContent = exportedFile.readText()

        // 3. Asercje Poprawności (Sprawdzamy czy plik XES ma zapisane zmiany)

        // Atrybuty, które miały ZOSTAĆ
        assertTrue(xmlContent.contains("Trace_A_Normal"), "Niewinny ślad zniknął!")
        assertTrue(xmlContent.contains("1500.5"), "Niewinna kwota zniknęła!")
        assertTrue(xmlContent.contains("Request Received"), "Zdarzenie początkowe oszukańczego śladu nie powinno było zostać usunięte!")

        // Atrybuty, które miały ZNIKNĄĆ
        assertFalse(xmlContent.contains("Block Account"), "BŁĄD: Usunięte zdarzenie wyciekło do pliku wyjściowego!")
        assertFalse(xmlContent.contains("99999.99"), "BŁĄD: Oszukańcza kwota wciąż jest w pliku!")
        assertFalse(xmlContent.contains("Suspicious IP"), "BŁĄD: Atrybuty usuniętego zdarzenia przetrwały!")

        println("✅ Mutation Test (PQL Delete -> Export) przeszedł pomyślnie. Zmiany prawidłowo zapisały się w nowym XML.")
    }
}
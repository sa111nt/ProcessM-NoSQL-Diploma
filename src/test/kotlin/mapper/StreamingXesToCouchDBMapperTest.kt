package mapper

import com.google.gson.JsonParser
import db.CouchDBManager
import input.parsers.XESLogAttributes
import input.parsers.XESTrace
import model.Event
import model.Trace
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.Mockito

class StreamingXesToCouchDBMapperTest {

    @Test
    @DisplayName("Mapper powinien generować poprawną strukturę JSON (Log, Trace, Event)")
    fun testJsonStructure() {
        // 1. Mockowanie bazy danych
        val mockDb = Mockito.mock(CouchDBManager::class.java)

        // 2. Dane wejściowe
        val logAttr = XESLogAttributes(mutableMapOf("source" to "Test"))

        val event = Event(
            id = "e1",
            traceId = "t1",
            name = "Activity A",
            timestamp = "2023-01-01",
            attributes = mutableMapOf("cost" to 100)
        )

        val trace = Trace(
            id = "t1",
            attributes = mutableMapOf("concept:name" to "Case1"),
            events = mutableListOf(event)
        )
        val traceComp = XESTrace(trace)

        val inputList = listOf(logAttr, traceComp)

        // 3. Uruchomienie mappera
        val mapper = StreamingXesToCouchDBMapper(mockDb, "test_db", batchSize = 10, parallelism = 1)
        mapper.map(inputList.iterator())

        // 4. Przechwycenie danych
        val captor = ArgumentCaptor.forClass(ByteArray::class.java)

        // ZMIANA: Używamy nowej metody captureByteArray, która zwraca pustą tablicę zamiast nulla
        Mockito.verify(mockDb, Mockito.atLeastOnce()).insertBulkDocsRaw(
            Mockito.anyString(),
            MockitoHelper.captureByteArray(captor)
        )

        // 5. Analiza JSONa
        val sentBytes = captor.value
        val jsonString = String(sentBytes, Charsets.UTF_8)

        val jsonRoot = JsonParser.parseString(jsonString).asJsonObject
        val docs = jsonRoot.getAsJsonArray("docs")

        assertEquals(3, docs.size())

        // Weryfikacja LOG
        val docLog = docs.first { it.asJsonObject.get("docType").asString == "log" }.asJsonObject
        assertNotNull(docLog.get("_id"))
        assertEquals("Test", docLog.get("log_attributes").asJsonObject.get("source").asString)

        // Weryfikacja TRACE
        val docTrace = docs.first { it.asJsonObject.get("docType").asString == "trace" }.asJsonObject
        assertTrue(docTrace.get("logId").asString.startsWith("log_"))
        assertEquals("t1", docTrace.get("originalTraceId").asString)

        // Weryfikacja EVENT
        val docEvent = docs.first { it.asJsonObject.get("docType").asString == "event" }.asJsonObject
        assertEquals("Activity A", docEvent.get("activity").asString)
        assertEquals("100", docEvent.get("xes_attributes").asJsonObject.get("cost").asString)
    }

    @Test
    @DisplayName("Mapper powinien wysyłać dane w paczkach (Batching)")
    fun testBatching() {
        val mockDb = Mockito.mock(CouchDBManager::class.java)

        // Generujemy 5 śladów
        val traces = (1..5).map {
            XESTrace(Trace("t$it", mutableMapOf(), mutableListOf()))
        }

        val input = (listOf(XESLogAttributes(mutableMapOf())) + traces).iterator()

        val mapper = StreamingXesToCouchDBMapper(mockDb, "test_db", batchSize = 2, parallelism = 1)
        mapper.map(input)

        // Tutaj też używamy bezpiecznego matchera
        Mockito.verify(mockDb, Mockito.times(3)).insertBulkDocsRaw(
            Mockito.anyString(),
            MockitoHelper.anyObject()
        )
    }
}

/**
 * Helper object to bypassing Kotlin's null-safety checks when using Mockito.
 */
object MockitoHelper {
    // Specjalna metoda dla ByteArray - zwraca pustą tablicę zamiast nulla,
    // żeby zadowolić Kotlina ("ByteArray" is non-nullable)
    fun captureByteArray(captor: ArgumentCaptor<ByteArray>): ByteArray {
        captor.capture()
        return ByteArray(0)
    }

    // Ogólna metoda dla innych typów referencyjnych (zwraca null)
    fun <T> capture(captor: ArgumentCaptor<T>): T {
        captor.capture()
        return uninitialized()
    }

    fun <T> anyObject(): T {
        Mockito.any<T>()
        return uninitialized()
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> uninitialized(): T = null as T
}
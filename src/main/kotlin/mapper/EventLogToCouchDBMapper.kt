package mapper

import com.google.gson.stream.JsonWriter
import input.parsers.XESComponent
import input.parsers.XESTrace
import input.parsers.XESLogAttributes
import db.CouchDBManager
import model.Event
import model.Trace
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

// Klasa odpowiedzialna za mapowanie strumienia obiektów XES na dokumenty JSON w CouchDB.
// Działa w modelu Producer-Consumer:
// - Główny wątek (Producer): Czyta XML i tworzy paczki (batches).
// - Wątki w tle (Consumers): Przetwarzają paczki na JSON i wysyłają do bazy.
class StreamingXesToCouchDBMapper(
    private val couchDB: CouchDBManager,
    private val databaseName: String,
    // Wielkość paczki. 1000 to balans między narzutem HTTP a zużyciem RAM.
    private val batchSize: Int = 1000,
    // Liczba wątków. Ustawiamy 6-8, aby nasycić rdzenie procesora podczas generowania JSON.
    private val parallelism: Int = 6
) {

    fun map(parser: Iterator<XESComponent>) {
        // 1. SETUP
        // Upewniamy się, że baza danych istnieje, zanim cokolwiek wyślemy.
        // POPRAWKA: Zmieniono nazwę metody na zgodną z CouchDBManager
        couchDB.createDb(databaseName)

        // Generujemy unikalne ID dla całego importu.
        // Dzięki temu wszystkie dokumenty z tego pliku będą logicznie powiązane.
        val logId = "log_${UUID.randomUUID()}"
        println("[INFO] Starting import for Log ID: $logId")

        // Tworzymy pulę wątków (FixedThreadPool).
        // Ogranicza to liczbę aktywnych wątków do 'parallelism', chroniąc przed zabiciem CPU.
        val executor = Executors.newFixedThreadPool(parallelism)

        val currentBatch = mutableListOf<XESComponent>()
        var logDocCreated = false

        // 2. PĘTLA GŁÓWNA (PRODUCER)
        // Ten fragment działa na głównym wątku (Main Thread).
        // Jego jedynym zadaniem jest szybkie czytanie XML i delegowanie pracy.
        parser.forEach { component ->

            // Logika segregacji: Atrybuty logu zapisujemy tylko raz
            if (component is XESLogAttributes && !logDocCreated) {
                currentBatch.add(component)
                logDocCreated = true
            } else if (component is XESTrace) {
                currentBatch.add(component)
            }

            // Mechanizm Batchingu:
            // Gdy uzbieramy 1000 elementów, kopiujemy listę i wysyłamy ją do wątku roboczego.
            if (currentBatch.size >= batchSize) {
                // Tworzymy kopię (toList), bo 'currentBatch' zaraz zostanie wyczyszczone.
                val batchCopy = currentBatch.toList()
                currentBatch.clear()

                // submit { ... }: To wrzuca zadanie na kolejkę Executora.
                // Główny wątek NIE czeka tutaj na zapis, leci od razu do następnego elementu XML.
                executor.submit { insertBatch(batchCopy, logId) }
            }
        }

        // 3. CZYSZCZENIE KOŃCÓWKI
        // Po wyjściu z pętli w batchu mogły zostać resztki (np. 350 elementów). Wysyłamy je.
        if (currentBatch.isNotEmpty()) {
            val batchCopy = currentBatch.toList()
            currentBatch.clear()
            executor.submit { insertBatch(batchCopy, logId) }
        }

        // 4. ZAMYKANIE I OCZEKIWANIE (SYNCHRONIZACJA)
        // Mówimy Executorowi: "Nie przyjmuj nowych zadań, ale dokończ te, które masz w kolejce".
        executor.shutdown()

        // KLUCZOWY ELEMENT DLA WERSJI SYNCHRONICZNEJ:
        // Główny wątek musi tu poczekać (zablokować się), aż wszystkie wątki robocze
        // skończą wysyłać dane do CouchDB. Bez tego program zakończyłby się natychmiast
        // po przeczytaniu XML, ubijając wątki w połowie wysyłania.
        try {
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                println("[ERROR] Timeout waiting for import threads to finish.")
                executor.shutdownNow() // Jeśli trwa to > 1h, zabijamy siłowo
            }
        } catch (e: InterruptedException) {
            executor.shutdownNow()
        }
        println("[INFO] Import finished.")
    }

    // 5. ZADANIE ROBOCZE (CONSUMER)
    // Ta metoda wykonuje się RÓWNOLEGLE na wielu wątkach.
    private fun insertBatch(components: List<XESComponent>, logId: String) {
        try {
            // Optymalizacja pamięci:
            // Zamiast tworzyć wielkie Stringi (String concatenation), używamy strumienia bajtów.
            // JsonWriter pisze bezpośrednio do bufora pamięci.
            val baos = ByteArrayOutputStream()
            val writer = JsonWriter(OutputStreamWriter(baos, StandardCharsets.UTF_8))

            // Struktura Bulk API CouchDB: { "docs": [ ... ] }
            writer.beginObject()
            writer.name("docs")
            writer.beginArray()

            components.forEach { comp ->
                when (comp) {
                    is XESLogAttributes -> writeLog(writer, comp, logId)
                    is XESTrace -> {
                        // XES Trace w modelu NoSQL rozbijamy na osobne dokumenty:
                        // 1. Dokument Śladu (Trace) - metadane
                        writeTrace(writer, comp.trace, logId)
                        // 2. Dokumenty Zdarzeń (Events) - każdy event to osobny JSON
                        comp.trace.events.forEach { writeEvent(writer, it, logId) }
                    }
                }
            }

            writer.endArray()
            writer.endObject()

            // Zrzucamy bufor do tablicy bajtów
            writer.flush()
            writer.close()

            // Wysyłamy gotowy JSON do CouchDB.
            // Metoda insertBulkDocsRaw przyjmuje ByteArray i wysyła POST.
            couchDB.insertBulkDocsRaw(databaseName, baos.toByteArray())

        } catch (e: Exception) {
            println("[ERROR] Error inserting batch: ${e.message}")
            e.printStackTrace()
        }
    }

    // --- METODY POMOCNICZE DO GENEROWANIA JSON (Gson Streaming) ---

    private fun writeLog(writer: JsonWriter, logAttr: XESLogAttributes, logId: String) {
        writer.beginObject()
        writer.name("_id").value(logId) // ID dokumentu = ID Logu
        writer.name("docType").value("log") // Dyskryminator typu (do zapytań SQL-like)
        writer.name("source").value(logAttr.attributes["source"]?.toString() ?: "unknown")
        writer.name("importTimestamp").value(System.currentTimeMillis())
        writer.name("log_attributes")
        writeAttributes(writer, logAttr.attributes)
        writer.endObject()
    }

    private fun writeTrace(writer: JsonWriter, trace: Trace, logId: String) {
        writer.beginObject()
        // ID Śladu: logId + traceId. Gwarantuje unikalność w bazie.
        writer.name("_id").value("${logId}_${trace.id}")
        writer.name("docType").value("trace")

        // Klucz obcy do rodzica (Logu)
        writer.name("logId").value(logId)
        writer.name("originalTraceId").value(trace.id)

        // Lista ID dzieci (Eventów) - opcjonalne, ale ułatwia nawigację w bazie
        writer.name("eventIds")
        writer.beginArray()
        trace.events.forEach { writer.value("${logId}_${it.id}") }
        writer.endArray()

        writer.name("xes_attributes")
        writeAttributes(writer, trace.attributes)
        writer.endObject()
    }

    private fun writeEvent(writer: JsonWriter, event: Event, logId: String) {
        writer.beginObject()
        // ID Zdarzenia: logId + eventId
        writer.name("_id").value("${logId}_${event.id}")
        writer.name("docType").value("event")

        // Denormalizacja: Event zna swojego Loga i swojego Trace'a.
        // Ułatwia to szybkie zapytania bez kosztownych złączeń (joins).
        writer.name("logId").value(logId)
        val namespacedTraceId = if (event.traceId != null) "${logId}_${event.traceId}" else null
        writer.name("traceId").value(namespacedTraceId)

        writer.name("activity").value(event.name)
        writer.name("timestamp").value(event.timestamp)
        writer.name("xes_attributes")
        writeAttributes(writer, event.attributes)
        writer.endObject()
    }

    private fun writeAttributes(writer: JsonWriter, attributes: Map<String, Any?>) {
        writer.beginObject()
        attributes.forEach { (k, v) ->
            // Konwertujemy wszystko na String dla bezpieczeństwa JSON
            writer.name(k).value(v?.toString())
        }
        writer.endObject()
    }
}
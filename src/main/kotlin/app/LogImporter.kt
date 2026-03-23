package app

import db.CouchDBManager
import input.LogFileDetector
import input.LogFileType
import input.parsers.StaxXesParser
import mapper.StreamingXesToCouchDBMapper
import java.io.File
import java.io.InputStream
import java.util.zip.GZIPInputStream
import java.util.zip.ZipInputStream
import javax.xml.stream.XMLInputFactory

object LogImporter {

    /**
     * Główna funkcja importująca.
     * @param filePath Ścieżka do pliku na dysku.
     * @param targetDbName Nazwa bazy danych w CouchDB (domyślnie "event_logs").
     * @param providedManager Opcjonalny manager. Jeśli null, funkcja stworzy własny i go zamknie.
     */
    fun import(
        filePath: String,
        targetDbName: String = "event_logs",
        providedManager: CouchDBManager? = null
    ) {
        val file = File(filePath)

        if (!file.exists()) {
            println("[ERROR] File not found: ${file.absolutePath}")
            return
        }

        val type = LogFileDetector.detect(file)

        // LOGIKA ZASOBÓW:
        val couch = providedManager ?: CouchDBManager()
        val shouldCloseManager = providedManager == null

        println("[INFO] Starting import for: ${file.name} [$type] -> DB: $targetDbName")
        val startTime = System.currentTimeMillis()

        try {
            when (type) {
                LogFileType.XES -> {
                    importStream(file.inputStream(), couch, targetDbName)
                }
                LogFileType.GZ -> {
                    GZIPInputStream(file.inputStream()).use { gzipStream ->
                        importStream(gzipStream, couch, targetDbName)
                    }
                }
                LogFileType.ZIP -> {
                    ZipInputStream(file.inputStream()).use { zipStream ->
                        var entry = zipStream.nextEntry
                        while (entry != null) {
                            if (!entry.isDirectory && entry.name.endsWith(".xes", ignoreCase = true)) {
                                println("[INFO] Found XES inside ZIP: ${entry.name}")
                                importStream(zipStream, couch, targetDbName)
                            }
                            entry = zipStream.nextEntry
                        }
                    }
                }
                else -> println("[WARN] Unknown or unsupported file format: ${file.name}")
            }
        } catch (e: Exception) {
            e.printStackTrace()
            println("[ERROR] Import failed: ${e.message}")
        } finally {
            if (shouldCloseManager) {
                couch.close()
            }
        }

        val endTime = System.currentTimeMillis()
        println("[INFO] Finished operation in ${(endTime - startTime) / 1000.0} s")
    }

    private fun importStream(inputStream: InputStream, couch: CouchDBManager, dbName: String) {
        val xmlReader = XMLInputFactory.newDefaultFactory().createXMLStreamReader(inputStream)
        val parser = StaxXesParser(xmlReader)

        val mapper = StreamingXesToCouchDBMapper(couch, dbName, parallelism = 6)
        mapper.map(parser)

        // --- OPTYMALIZACJA NOSQL (TWORZENIE INDEKSÓW) ---
        // Po udanym wgraniu dokumentów zmuszamy bazę do utworzenia struktur B-Tree (indeksów).
        // Dzięki temu zapytania PQL oraz Orkiestrator N+1 będą działać w ułamku sekundy,
        // eliminując konieczność skanowania całej bazy (tzw. Full Collection Scan).
        println("[INFO] Building database indexes for optimal query performance...")

        // Indeks do błyskawicznego odróżniania Eventów od Logów/Trace'ów
        couch.ensureIndex(dbName, listOf("docType"))
        // Indeks KLUCZOWY dla trybu N+1 (pobieranie Śladów na podstawie Logu)
        couch.ensureIndex(dbName, listOf("logId"))
        // Indeks KLUCZOWY dla trybu N+1 (pobieranie Zdarzeń na podstawie Śladu)
        couch.ensureIndex(dbName, listOf("traceId"))
        // Indeks dla najczęstszego atrybutu domenowego (PQL where e:name = ...)
        couch.ensureIndex(dbName, listOf("activity"))
        // Indeks ułatwiający sortowanie czasowe
        couch.ensureIndex(dbName, listOf("timestamp"))

        // NOWE: Błyskawiczne wyszukiwanie po ORYGINALNYM ID z pliku XES (Rozwiązanie Problemu 7)
        couch.ensureIndex(dbName, listOf("identity:id"))
        couch.ensureIndex(dbName, listOf("log_attributes.identity:id"))

        println("[INFO] Indexes successfully created or verified!")
    }
}
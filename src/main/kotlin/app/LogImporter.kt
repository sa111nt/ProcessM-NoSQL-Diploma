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
        // Jeśli otrzymaliśmy manager z zewnątrz (np. z Main.kt), używamy go.
        // Jeśli nie, tworzymy własny tymczasowy.
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
                                // Przekazujemy strumień ZIP bez zamykania go tutaj (zamknie go blok use)
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
            // Zamykamy manager TYLKO wtedy, gdy sami go stworzyliśmy.
            // Jeśli przyszedł z zewnątrz, niech właściciel (Main) decyduje kiedy go zamknąć.
            if (shouldCloseManager) {
                couch.close()
            }
        }

        val endTime = System.currentTimeMillis()
        println("[INFO] Finished operation in ${(endTime - startTime) / 1000.0} s")
    }

    private fun importStream(inputStream: InputStream, couch: CouchDBManager, dbName: String) {
        // WAŻNE: Nie zamykamy inputStream tutaj, bo może to być ZipInputStream, który ma kolejne wpisy!
        // XMLInputFactory stworzy reader, który "pożycza" strumień.
        val xmlReader = XMLInputFactory.newDefaultFactory().createXMLStreamReader(inputStream)
        val parser = StaxXesParser(xmlReader)

        // Używamy przekazanej nazwy bazy (dbName) zamiast sztywnego "event_logs"
        val mapper = StreamingXesToCouchDBMapper(couch, dbName, batchSize = 100, parallelism = 6)

        mapper.map(parser)
    }
}
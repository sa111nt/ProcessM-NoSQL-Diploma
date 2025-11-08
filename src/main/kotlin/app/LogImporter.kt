package app

import input.LogFileDetector
import input.LogFileType
import input.ZipExtractor
import input.parsers.SaxXesParser
import mapper.EventLogToCouchDBMapper
import db.CouchDBManager
import java.io.File

/**
 * Główna funkcja odpowiedzialna za import logów (XES lub ZIP) do CouchDB.
 * Można ją wywołać z dowolnego miejsca (np. z main.kt lub testów).
 */
object LogImporter {

    fun import(filePath: String) {
        val file = File(filePath)
        if (!file.exists()) {
            println("❌ File not found: ${file.absolutePath}")
            return
        }

        val type = LogFileDetector.detect(file)
        val couch = CouchDBManager()
        val mapper = EventLogToCouchDBMapper(couch, "event_logs")
        val zipExtractor = ZipExtractor()

        when (type) {
            LogFileType.XES -> importXes(file, mapper)
            LogFileType.ZIP -> importZip(file, zipExtractor, mapper)
            else -> println("❓ Unknown or unsupported file format: ${file.name}")
        }
    }

    private fun importXes(file: File, mapper: EventLogToCouchDBMapper) {
        println("📄 Detected XES file: ${file.name}")
        try {
            val parser = SaxXesParser()
            val log = parser.parse(file)
            mapper.mapEventLogToCouchDB(log)
            println("✅ Successfully imported XES log: ${file.name}")
        } catch (e: Exception) {
            println("❌ Failed to parse XES file: ${file.name}")
            e.printStackTrace()
        }
    }

    private fun importZip(file: File, zipExtractor: ZipExtractor, mapper: EventLogToCouchDBMapper) {
        println("📦 Detected ZIP — extracting files...")
        val extractedFiles = zipExtractor.extractLogFiles(file)
        println("📝 Extracted ${extractedFiles.size} log file(s).")

        extractedFiles.forEach { extractedFile ->
            val innerType = LogFileDetector.detect(extractedFile)
            when (innerType) {
                LogFileType.XES -> {
                    println("➡️ Parsing XES inside ZIP: ${extractedFile.name}")
                    try {
                        val parser = SaxXesParser()
                        val log = parser.parse(extractedFile)
                        mapper.mapEventLogToCouchDB(log)
                        println("✅ Imported ${extractedFile.name}")
                    } catch (e: Exception) {
                        println("❌ Failed to parse ${extractedFile.name}: ${e.message}")
                        e.printStackTrace()
                    }
                }

                else -> println("⚠️ Unsupported file inside ZIP: ${extractedFile.name}")
            }
        }
    }
}
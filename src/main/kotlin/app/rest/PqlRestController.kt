package app.rest

import app.LogImporter
import app.PqlInterpreter
import app.XesExporter
import db.CouchDBManager
import org.springframework.core.io.FileSystemResource
import org.springframework.core.io.Resource
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import org.springframework.web.multipart.MultipartFile
import java.io.File
import java.util.UUID

@RestController
@RequestMapping("/api/pql")
@CrossOrigin(origins = ["*"])
class PqlRestController {

    private val dbManager = CouchDBManager("http://127.0.0.1:5984", "admin", "admin")
    private val dbName = "event_logs"
    private val interpreter = PqlInterpreter(dbManager, dbName)
    private val exporter = XesExporter(dbManager, dbName)

    init {
        try { dbManager.createDb(dbName) } catch (e: Exception) { }
    }

    /**
     * 1. ENDPOINT: Zapytania PQL
     */
    @PostMapping("/query", consumes = [MediaType.TEXT_PLAIN_VALUE], produces = [MediaType.APPLICATION_JSON_VALUE])
    fun executeQuery(@RequestBody query: String): ResponseEntity<String> {
        return try {
            println("Otrzymano zapytanie PQL: $query")
            val resultJsonArray = interpreter.executeQuery(query).asJsonArray
            ResponseEntity.ok(resultJsonArray.toString())
        } catch (e: Exception) {
            val errorJson = """{"error": "${e.message?.replace("\"", "\\\"")}"}"""
            ResponseEntity.badRequest().body(errorJson)
        }
    }

    /**
     * 2. ENDPOINT: Import pliku XES (Zwraca ID wgranego logu)
     */
    @PostMapping("/import")
    fun importLog(@RequestParam("file") file: MultipartFile): ResponseEntity<String> {
        return try {
            val tempFile = File.createTempFile("upload_", "_" + file.originalFilename)
            file.transferTo(tempFile)

            // Wywołujemy importer
            LogImporter.import(tempFile.absolutePath, dbName, dbManager)
            tempFile.delete()

            // ROZWIĄZANIE BŁĘDU 400: Pobieramy logi bez użycia klauzuli 'sort' w CouchDB
            val logQuery = """{"selector": {"docType": "log"}, "limit": 100}"""
            val logs = dbManager.findDocs(dbName, com.google.gson.JsonParser.parseString(logQuery).asJsonObject)

            // Wyszukujemy najnowszy log w pamięci (bezpieczne i błyskawiczne)
            var latestLogId = "unknown"
            var maxTs = 0L
            for (i in 0 until logs.size()) {
                val logObj = logs[i].asJsonObject
                val ts = logObj.get("importTimestamp")?.asLong ?: 0L
                if (ts >= maxTs) {
                    maxTs = ts
                    latestLogId = logObj.get("_id").asString
                }
            }

            ResponseEntity.ok("""{"status": "SUCCESS", "message": "Zaimportowano pomyślnie", "logId": "$latestLogId"}""")
        } catch (e: Exception) {
            ResponseEntity.badRequest().body("""{"status": "ERROR", "message": "${e.message}"}""")
        }
    }

    /**
     * 3. ENDPOINT: Eksport do XES (Wymusza pobieranie pliku)
     */
    @GetMapping("/export/{logId}")
    fun exportLog(@PathVariable logId: String): ResponseEntity<Resource> {
        return try {
            val outputPath = "export_${UUID.randomUUID()}.xes"
            exporter.exportToFile(logId, outputPath)

            val file = File(outputPath)
            val resource = FileSystemResource(file)

            ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"${logId}.xes\"")
                .contentType(MediaType.APPLICATION_XML)
                .body(resource)
        } catch (e: Exception) {
            ResponseEntity.badRequest().build()
        }
    }

    /**
     * 4. ENDPOINT: Całkowite czyszczenie bazy danych (Usuwa wszystkie logi)
     */
    @DeleteMapping("/deleteAll")
    fun deleteAllLogs(): ResponseEntity<String> {
        return try {
            println("Rozpoczęto całkowite czyszczenie bazy danych...")
            // Najszybszy sposób na wyczyszczenie NoSQL: usunięcie bazy i założenie jej na nowo
            dbManager.deleteDb(dbName)
            dbManager.createDb(dbName)

            ResponseEntity.ok("""{"status": "SUCCESS", "message": "Baza danych została pomyślnie wyczyszczona."}""")
        } catch (e: Exception) {
            ResponseEntity.badRequest().body("""{"status": "ERROR", "message": "${e.message}"}""")
        }
    }
}
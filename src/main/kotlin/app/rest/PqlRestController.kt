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

    @PostMapping("/query", consumes = [MediaType.TEXT_PLAIN_VALUE], produces = [MediaType.APPLICATION_JSON_VALUE])
    fun executeQuery(@RequestBody query: String): ResponseEntity<String> {
        return try {
            val resultJsonArray = interpreter.executeQuery(query).asJsonArray
            ResponseEntity.ok(resultJsonArray.toString())
        } catch (e: Exception) {
            val errorJson = """{"error": "${e.message?.replace("\"", "\\\"")}"}"""
            ResponseEntity.badRequest().body(errorJson)
        }
    }

    @PostMapping("/import")
    fun importLog(@RequestParam("file") file: MultipartFile): ResponseEntity<String> {
        return try {
            val tempFile = File.createTempFile("upload_", "_" + file.originalFilename)
            file.transferTo(tempFile)

            LogImporter.import(tempFile.absolutePath, dbName, dbManager)
            tempFile.delete()

            val logQuery = """{"selector": {"docType": "log"}, "limit": 100}"""
            val logs = dbManager.findDocs(dbName, com.google.gson.JsonParser.parseString(logQuery).asJsonObject)

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

    @DeleteMapping("/deleteAll")
    fun deleteAllLogs(): ResponseEntity<String> {
        return try {
            dbManager.deleteDb(dbName)
            dbManager.createDb(dbName)

            ResponseEntity.ok("""{"status": "SUCCESS", "message": "Baza danych została pomyślnie wyczyszczona."}""")
        } catch (e: Exception) {
            ResponseEntity.badRequest().body("""{"status": "ERROR", "message": "${e.message}"}""")
        }
    }
}
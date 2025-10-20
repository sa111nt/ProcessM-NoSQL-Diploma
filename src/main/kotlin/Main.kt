import input.LogFileDetector
import input.LogFileType
import input.ZipExtractor
import input.parsers.OcelJsonParser
import input.parsers.XesParser
import mapper.EventLogToCouchDBMapper
import db.CouchDBManager
import java.io.File

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        println("❌ Please provide a path to a log file or ZIP as an argument.")
        return
    }

    val file = File(args[0])
    if (!file.exists()) {
        println("❌ File not found: ${file.absolutePath}")
        return
    }

    val type = LogFileDetector.detect(file)
    val couch = CouchDBManager()
    val mapper = EventLogToCouchDBMapper(couch, "event_logs")
    val zipExtractor = ZipExtractor()

    when (type) {
        LogFileType.OCEL_JSON -> {
            println("📄 Detected OCEL JSON: ${file.name}")
            try {
                val parser = OcelJsonParser()
                val log = parser.parse(file)
                mapper.mapEventLogToCouchDB(log)
                println("✅ Successfully imported OCEL JSON log.")
            } catch (e: Exception) {
                println("❌ Failed to parse OCEL JSON: ${e.message}")
                e.printStackTrace()
            }
        }

        LogFileType.XES -> {
            println("📄 Detected XES file: ${file.name}")
            try {
                val parser = XesParser()
                val log = parser.parse(file)
                mapper.mapEventLogToCouchDB(log)
                println("✅ Successfully imported XES log.")
            } catch (e: Exception) {
                println("❌ Failed to parse XES: ${e.message}")
                e.printStackTrace()
            }
        }

        LogFileType.ZIP -> {
            println("📦 Detected ZIP — extracting files...")
            val extractedFiles = zipExtractor.extractLogFiles(file)
            println("📝 Extracted ${extractedFiles.size} log file(s).")

            extractedFiles.forEach { extractedFile ->
                val innerType = LogFileDetector.detect(extractedFile)
                when (innerType) {
                    LogFileType.OCEL_JSON -> {
                        println("➡️ Parsing OCEL JSON inside ZIP: ${extractedFile.name}")
                        try {
                            val parser = OcelJsonParser()
                            val log = parser.parse(extractedFile)
                            mapper.mapEventLogToCouchDB(log)
                            println("✅ Imported ${extractedFile.name}")
                        } catch (e: Exception) {
                            println("❌ Failed to parse ${extractedFile.name}: ${e.message}")
                        }
                    }

                    LogFileType.XES -> {
                        println("➡️ Parsing XES inside ZIP: ${extractedFile.name}")
                        try {
                            val parser = XesParser()
                            val log = parser.parse(extractedFile)
                            mapper.mapEventLogToCouchDB(log)
                            println("✅ Imported ${extractedFile.name}")
                        } catch (e: Exception) {
                            println("❌ Failed to parse ${extractedFile.name}: ${e.message}")
                        }
                    }

                    else -> println("⚠️ Unsupported file inside ZIP: ${extractedFile.name}")
                }
            }
        }

        else -> println("❓ Unknown file format: ${file.name}")
    }
}
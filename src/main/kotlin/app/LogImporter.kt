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

    fun import(
        filePath: String,
        targetDbName: String = "event_logs",
        providedManager: CouchDBManager? = null
    ) {
        val file = File(filePath)

        if (!file.exists()) {
            return
        }

        val type = LogFileDetector.detect(file)
        val couch = providedManager ?: CouchDBManager()
        val shouldCloseManager = providedManager == null

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
                                importStream(zipStream, couch, targetDbName)
                            }
                            entry = zipStream.nextEntry
                        }
                    }
                }
                else -> { }
            }
        } catch (e: Exception) {
            e.printStackTrace()
        } finally {
            if (shouldCloseManager) {
                couch.close()
            }
        }
    }

    private fun importStream(inputStream: InputStream, couch: CouchDBManager, dbName: String) {
        val xmlReader = XMLInputFactory.newDefaultFactory().createXMLStreamReader(inputStream)
        val parser = StaxXesParser(xmlReader)

        val mapper = StreamingXesToCouchDBMapper(couch, dbName, parallelism = 6)
        mapper.map(parser)

        couch.ensureIndex(dbName, listOf("docType"))
        couch.ensureIndex(dbName, listOf("logId"))
        couch.ensureIndex(dbName, listOf("traceId"))
        couch.ensureIndex(dbName, listOf("activity"))
        couch.ensureIndex(dbName, listOf("timestamp"))
        couch.ensureIndex(dbName, listOf("identity:id"))
        couch.ensureIndex(dbName, listOf("log_attributes.identity:id"))
    }
}
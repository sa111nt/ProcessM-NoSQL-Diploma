package app

import com.google.gson.JsonObject
import db.CouchDBManager
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.w3c.dom.Element
import java.io.File
import javax.xml.parsers.DocumentBuilderFactory

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class XesExporterTest {

    private lateinit var dbManager: CouchDBManager
    private lateinit var interpreter: PqlInterpreter
    private lateinit var exporter: XesExporter
    private val dbName = "event_logs_export_test"

    private val inputFilePath = "src/test/resources/generated_complex_test.xes"
    private val outputIdentityPath = "src/test/resources/exported_identity_test.xes"
    private val outputDeletePath = "src/test/resources/exported_delete_test.xes"
    private val outputLargePath = "src/test/resources/exported_large_test.xes"

    @BeforeAll
    fun setup() {
        dbManager = CouchDBManager("http://127.0.0.1:5984", "admin", "admin")
        try { dbManager.deleteDb(dbName) } catch (e: Exception) {}
        try { dbManager.createDb(dbName) } catch (e: Exception) {}

        interpreter = PqlInterpreter(dbManager, dbName)
        exporter = XesExporter(dbManager, dbName)

        File("src/test/resources").mkdirs()
    }

    private fun createComplexXesFile() {
        val xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <log xes.version="1.0" xmlns="http://www.xes-standard.org/">
              <extension name="Concept" prefix="concept" uri="http://www.xes-standard.org/concept.xesext"/>
              <extension name="Time" prefix="time" uri="http://www.xes-standard.org/time.xesext"/>
              <extension name="Organizational" prefix="org" uri="http://www.xes-standard.org/org.xesext"/>
              
              <global scope="trace">
                <string key="concept:name" value="UNKNOWN"/>
              </global>
              <global scope="event">
                <string key="concept:name" value="UNKNOWN"/>
                <string key="org:resource" value="SYSTEM"/>
              </global>
              
              <classifier name="Activity" keys="concept:name"/>
              <classifier name="Activity and Resource" keys="concept:name org:resource"/>
              
              <string key="concept:name" value="Complex Bank Process"/>
              <id key="identity:id" value="log-999-uuid"/>
              <int key="version_code" value="5"/>
              
              <trace>
                <string key="concept:name" value="Trace_A_Normal"/>
                <int key="customer_id" value="101"/>
                <id key="identity:id" value="trace-uuid-1"/>
                
                <list key="audit_trail">
                    <string key="0" value="verified_by_system"/>
                    <date key="1" value="2023-10-01T09:00:00Z"/>
                </list>
                <container key="meta_data">
                    <boolean key="is_archived" value="false"/>
                    <float key="confidence_score" value="99.9"/>
                </container>
                
                <event>
                  <string key="concept:name" value="Request Received"/>
                  <date key="time:timestamp" value="2023-10-01T10:00:00Z"/>
                  <string key="org:resource" value="System"/>
                  <boolean key="is_automated" value="true"/>
                  <id key="identity:id" value="evt-uuid-1"/>
                </event>
                <event>
                  <string key="concept:name" value="Funds Deducted"/>
                  <date key="time:timestamp" value="2023-10-01T10:15:00Z"/>
                  <float key="cost:amount" value="1500.5"/>
                  <boolean key="is_fraud" value="false"/>
                  <id key="identity:id" value="evt-uuid-2"/>
                </event>
              </trace>
              
              <trace>
                <string key="concept:name" value="Trace_B_Fraud"/>
                <int key="customer_id" value="202"/>
                <id key="identity:id" value="trace-uuid-2"/>
                <event>
                  <string key="concept:name" value="Request Received"/>
                  <date key="time:timestamp" value="2023-10-02T11:00:00Z"/>
                  <boolean key="is_automated" value="false"/>
                  <id key="identity:id" value="evt-uuid-3"/>
                </event>
                <event>
                  <string key="concept:name" value="Block Account"/>
                  <date key="time:timestamp" value="2023-10-02T11:05:00Z"/>
                  <float key="cost:amount" value="99999.99"/>
                  <boolean key="is_fraud" value="true"/>
                  <string key="reason" value="Suspicious IP"/>
                  <id key="identity:id" value="evt-uuid-4"/>
                </event>
              </trace>
            </log>
        """.trimIndent()
        val testFile = File(inputFilePath)
        if (testFile.exists()) testFile.delete()
        testFile.writeText(xml)
    }

    private fun getLogIdFromDb(): String {
        val logQuery = JsonObject().apply {
            add("selector", JsonObject().apply { addProperty("docType", "log") })
        }
        val logs = dbManager.findDocs(dbName, logQuery)
        assertTrue(logs.size() > 0)
        return logs[0].asJsonObject.get("_id").asString
    }

    data class XesAttribute(val type: String, val key: String, val value: String?, val children: Set<XesAttribute> = emptySet())
    data class XesExtension(val name: String, val prefix: String, val uri: String)
    data class XesGlobal(val scope: String, val attributes: Set<XesAttribute>)
    data class XesClassifier(val name: String, val keys: String)
    data class XesEvent(val attributes: Set<XesAttribute>)
    data class XesTrace(val attributes: Set<XesAttribute>, val events: List<XesEvent>)
    data class XesLog(
        val attributes: Set<XesAttribute>,
        val extensions: Set<XesExtension>,
        val globals: Set<XesGlobal>,
        val classifiers: Set<XesClassifier>,
        val traces: Set<XesTrace>
    )

    private fun parseXesToModel(filePath: String): XesLog {
        val factory = DocumentBuilderFactory.newInstance()
        val builder = factory.newDocumentBuilder()
        val doc = builder.parse(File(filePath))
        doc.documentElement.normalize()

        val logElement = doc.getElementsByTagName("log").item(0) as Element

        fun getDirectChildElements(parent: Element, tagName: String): List<Element> {
            val result = mutableListOf<Element>()
            val nodes = parent.childNodes
            for (i in 0 until nodes.length) {
                val node = nodes.item(i)
                if (node is Element && node.tagName == tagName) result.add(node)
            }
            return result
        }

        fun extractAttributes(element: Element): Set<XesAttribute> {
            val attrs = mutableSetOf<XesAttribute>()
            val simpleTypes = listOf("string", "date", "int", "float", "boolean", "id")
            val complexTypes = listOf("list", "container")

            val nodes = element.childNodes
            for (i in 0 until nodes.length) {
                val node = nodes.item(i)
                if (node is Element) {
                    val key = node.getAttribute("key")
                    if (simpleTypes.contains(node.tagName)) {
                        var value = node.getAttribute("value")
                        if (node.tagName == "float") value = value.toDoubleOrNull()?.toString() ?: value
                        attrs.add(XesAttribute(node.tagName, key, value))
                    } else if (complexTypes.contains(node.tagName)) {
                        val children = extractAttributes(node)
                        attrs.add(XesAttribute(node.tagName, key, null, children))
                    }
                }
            }
            return attrs
        }

        val extensions = getDirectChildElements(logElement, "extension").map {
            XesExtension(it.getAttribute("name"), it.getAttribute("prefix"), it.getAttribute("uri"))
        }.toSet()

        val globals = getDirectChildElements(logElement, "global").map {
            XesGlobal(it.getAttribute("scope"), extractAttributes(it))
        }.toSet()

        val classifiers = getDirectChildElements(logElement, "classifier").map {
            XesClassifier(it.getAttribute("name"), it.getAttribute("keys"))
        }.toSet()

        val logAttrs = extractAttributes(logElement)

        val traces = getDirectChildElements(logElement, "trace").map { tNode ->
            val tAttrs = extractAttributes(tNode)
            val events = getDirectChildElements(tNode, "event").map { eNode ->
                XesEvent(extractAttributes(eNode))
            }
            XesTrace(tAttrs, events)
        }.toSet()

        return XesLog(logAttrs, extensions, globals, classifiers, traces)
    }

    @Test
    fun testDeepSemanticIdentityExport() {
        createComplexXesFile()
        LogImporter.import(inputFilePath, dbName, dbManager)
        val logId = getLogIdFromDb()

        exporter.exportToFile(logId, outputIdentityPath)
        val exportedFile = File(outputIdentityPath)
        assertTrue(exportedFile.exists())

        val originalXes = parseXesToModel(inputFilePath)
        val exportedXes = parseXesToModel(outputIdentityPath)

        assertEquals(originalXes.extensions, exportedXes.extensions)
        assertEquals(originalXes.globals, exportedXes.globals)
        assertEquals(originalXes.classifiers, exportedXes.classifiers)
        assertEquals(originalXes.attributes, exportedXes.attributes)
        assertEquals(originalXes.traces.size, exportedXes.traces.size)

        originalXes.traces.forEach { originalTrace ->
            val matchingExportedTrace = exportedXes.traces.find { it.attributes == originalTrace.attributes }
            assertNotNull(matchingExportedTrace)
            assertEquals(originalTrace.events.size, matchingExportedTrace!!.events.size)
            for (i in originalTrace.events.indices) {
                val origEvent = originalTrace.events[i]
                val expEvent = matchingExportedTrace.events[i]
                assertEquals(origEvent.attributes, expEvent.attributes)
            }
        }

        try { dbManager.deleteDb(dbName); dbManager.createDb(dbName) } catch (e: Exception) {}
    }

    @Test
    fun testExportAfterPqlDelete() {
        createComplexXesFile()
        LogImporter.import(inputFilePath, dbName, dbManager)
        val logId = getLogIdFromDb()

        val deleteQuery = "delete event where l:_id='$logId' and e:name = 'Block Account'"
        val deleteResult = interpreter.executeQuery(deleteQuery).asJsonArray
        assertTrue(deleteResult[0].asJsonObject.get("deleted").asInt > 0)

        exporter.exportToFile(logId, outputDeletePath)
        val exportedFile = File(outputDeletePath)
        assertTrue(exportedFile.exists())

        val exportedXes = parseXesToModel(outputDeletePath)
        assertEquals(2, exportedXes.traces.size)

        val fraudTrace = exportedXes.traces.find { trace ->
            trace.attributes.any { it.key == "concept:name" && it.value == "Trace_B_Fraud" }
        }
        assertNotNull(fraudTrace)
        assertEquals(1, fraudTrace!!.events.size)

        val survivingEventName = fraudTrace.events[0].attributes.find { it.key == "concept:name" }?.value
        assertEquals("Request Received", survivingEventName)
    }

    @Test
    fun testJournalReviewExportEquivalence() {
        try { dbManager.deleteDb(dbName); dbManager.createDb(dbName) } catch (e: Exception) {}

        val journalPath = "src/test/resources/JournalReview-extra.xes"
        val exportPath = "src/test/resources/exported_journal_review.xes"

        LogImporter.import(journalPath, dbName, dbManager)
        val logId = getLogIdFromDb()

        val exportFile = File(exportPath)
        if (exportFile.exists()) exportFile.delete()
        exporter.exportToFile(logId, exportFile.absolutePath)

        assertTrue(exportFile.exists())

        val originalContent = File(journalPath).readText()
        val exportedContent = exportFile.readText()

        val originalTracesCount = originalContent.split("<trace>").size - 1
        val exportedTracesCount = exportedContent.split("<trace>").size - 1
        assertEquals(originalTracesCount, exportedTracesCount)

        val originalEventsCount = originalContent.split("<event>").size - 1
        val exportedEventsCount = exportedContent.split("<event>").size - 1
        assertEquals(originalEventsCount, exportedEventsCount)

        exportFile.delete()
        try { dbManager.deleteDb(dbName); dbManager.createDb(dbName) } catch (e: Exception) {}
    }

    @Test
    fun testLargeFileMemoryEfficiency() {
        val largeLogPath = "src/test/resources/Hospital_log.xes.gz"
        val exportFile = File(outputLargePath)

        if (!File(largeLogPath).exists()) {
            return
        }

        try { dbManager.deleteDb(dbName); dbManager.createDb(dbName) } catch (e: Exception) {}

        LogImporter.import(largeLogPath, dbName, dbManager)
        val logId = getLogIdFromDb()

        try {
            exporter.exportToFile(logId, exportFile.absolutePath)
            assertTrue(exportFile.exists())
            assertTrue(exportFile.length() > 1024 * 1024)
        } catch (e: OutOfMemoryError) {
            fail("OutOfMemoryError encountered during large file export")
        } finally {
            if (exportFile.exists()) exportFile.delete()
            try { dbManager.deleteDb(dbName); dbManager.createDb(dbName) } catch (e: Exception) {}
        }
    }
}
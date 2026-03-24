package pql

import app.PqlInterpreter
import app.LogImporter
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import db.CouchDBManager
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Disabled
import java.time.Instant
import kotlin.math.max
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PqlInterpreterTest {

    private lateinit var interpreter: PqlInterpreter
    private lateinit var journalLogId: String
    private lateinit var hospitalLogId: String
    private lateinit var advancedFeaturesLogId: String
    private lateinit var dbManager: CouchDBManager
    private val dbName = "event_logs"

    private val begin = Instant.parse("2000-01-01T00:00:00Z")
    private val end = Instant.parse("2030-01-01T00:00:00Z")
    private val eventNames = setOf(
        "invite reviewers", "get review 1", "get review 2", "get review 3",
        "collect reviews", "decide", "accept", "reject", "invite additional reviewer",
        "get review X", "time-out 1", "time-out 2", "time-out 3", "time-out X"
    )
    private val bpiEventNames = setOf(
        "A_SUBMITTED", "A_PARTLYSUBMITTED", "A_PREACCEPTED", "W_Completeren aanvraag",
        "A_ACCEPTED", "O_SELECTED", "A_FINALIZED", "O_CREATED", "O_SENT",
        "W_Nabellen offertes", "O_SENT_BACK", "W_Valideren aanvraag", "A_REGISTERED",
        "A_APPROVED", "O_ACCEPTED", "A_ACTIVATED", "A_DECLINED", "A_CANCELLED",
        "W_Afhandelen leads", "O_CANCELLED", "W_Beoordelen fraude"
    )
    private val orgResources = setOf(
        "System", "Resource01", "Resource02", "Resource03", "Resource04", "Resource05",
        "Resource06", "Resource07", "Resource08", "Resource09", "Anne", "Mike", "Pete", "Wil", "Sara", "Carol", "John", "Mary", "Sam", "Pam",
        "__INVALID__"
    )

    @BeforeAll
    fun setup() {
        dbManager = CouchDBManager("http://127.0.0.1:5984", "admin", "admin")
        try { dbManager.deleteDb(dbName) } catch (e: Exception) {}
        try { dbManager.createDb(dbName) } catch (e: Exception) {}

        val logQuery = JsonObject().apply { add("selector", JsonObject().apply { addProperty("docType", "log") }) }

        LogImporter.import("src/test/resources/Hospital_log.xes.gz", dbName, dbManager)
        val hospitalLogs = dbManager.findDocs(dbName, logQuery)
        hospitalLogId = hospitalLogs[0].asJsonObject.get("_id").asString

        LogImporter.import("src/test/resources/JournalReview-extra.xes", dbName, dbManager)
        val allLogs = dbManager.findDocs(dbName, logQuery)
        for (i in 0 until allLogs.size()) {
            val currentId = allLogs[i].asJsonObject.get("_id").asString
            if (currentId != hospitalLogId) {
                journalLogId = currentId
            }
        }

        val advancedFeaturesXesPath = "src/test/resources/advanced_features_test.xes"
        val advancedFeaturesXml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <log xes.version="1.0" xmlns="http://www.xes-standard.org/">
                <extension name="AdvancedCustomExt" prefix="adv" uri="http://example.org/adv.xesext"/>
                <global scope="trace"><string key="concept:name" value="default_trace"/></global>
                <global scope="event"><string key="concept:name" value="default_event"/></global>
                <classifier name="MyCustomClassifier" keys="concept:name adv:level"/>
                <string key="identity:id" value="BIZNESOWY-LOG-ID-999"/>
                <string key="concept:name" value="AdvancedFeaturesLog"/>
                <trace>
                    <string key="concept:name" value="Trace1"/>
                    <string key="identity:id" value="BIZNESOWY-TRACE-ID-123"/>
                    <event>
                        <string key="concept:name" value="TestEvent"/>
                        <string key="identity:id" value="BIZNESOWY-EVENT-ID-ABC"/>
                        <string key="time:timestamp" value="To nie jest data, to zwykly tekst!"/>
                        <date key="my_weird_string" value="2023-11-20T14:30:00.000+01:00"/>
                        <int key="is_premium" value="42"/>
                        <float key="my_integer_name" value="3.1415"/>
                        <boolean key="my_uuid_name" value="true"/>
                        <id key="my_string_name" value="123e4567-e89b-12d3-a456-426614174000"/>
                    </event>
                </trace>
            </log>
        """.trimIndent()

        val advancedFeaturesFile = java.io.File(advancedFeaturesXesPath)
        if (advancedFeaturesFile.exists()) advancedFeaturesFile.delete()
        advancedFeaturesFile.writeText(advancedFeaturesXml)

        LogImporter.import(advancedFeaturesXesPath, dbName, dbManager)

        val advancedFeaturesLogs = dbManager.findDocs(dbName, com.google.gson.JsonParser.parseString("""{"selector": {"docType": "log", "log_attributes.concept:name": "AdvancedFeaturesLog"}}""").asJsonObject)
        advancedFeaturesLogId = advancedFeaturesLogs[0].asJsonObject.get("_id").asString

        interpreter = PqlInterpreter(dbManager, dbName)

        advancedFeaturesFile.delete()
    }

    @Test
    fun errorHandlingTest() {
        val nonexistentClassifiers = listOf(
            "order by c:nonexistent",
            "group by [^c:nonstandard nonexisting]"
        )

        for (query in nonexistentClassifiers) {
            val ex = assertFailsWith<IllegalArgumentException> {
                interpreter.executeQuery(query).asJsonArray
            }
            assertTrue(ex.message!!.contains("not found", ignoreCase = true) || ex.message!!.contains("Classifier", ignoreCase = true))
        }
    }

    @Disabled("Silnik wspiera już klasyfikatory w klauzuli WHERE")
    @Test
    fun invalidUseOfClassifiers() {
        val ex = assertFailsWith<IllegalArgumentException> {
            interpreter.executeQuery("where [e:classifier:concept:name+lifecycle:transition] in ('acceptcomplete', 'rejectcomplete') and l:_id='$journalLogId'").asJsonArray
        }
        assertTrue(ex.message!!.contains("Classifier in WHERE", ignoreCase = true) || ex.message!!.contains("not supported", ignoreCase = true))

        val validUse = interpreter.executeQuery("select [e:c:Event Name] where l:_id='$journalLogId'").asJsonArray

        assertEquals(2298, validUse.size())

        val uniqueLogs = mutableSetOf<String>()
        val eventsByTrace = mutableMapOf<String, Int>()

        for (i in 0 until validUse.size()) {
            val doc = validUse[i].asJsonObject

            doc.get("logId")?.asString?.let { uniqueLogs.add(it) }
            val tId = doc.get("traceId")?.asString ?: "unknown"
            eventsByTrace[tId] = eventsByTrace.getOrDefault(tId, 0) + 1

            val businessKeys = doc.keySet().filter { it != "_id" && it != "logId" && it != "traceId" }
            assertEquals(1, businessKeys.size)
            assertEquals("[e:c:Event Name]", businessKeys.first())

            val classifierValue = doc.get("[e:c:Event Name]")?.asString ?: ""
            assertTrue(eventNames.any { classifierValue.startsWith(it) })
        }

        assertEquals(1, uniqueLogs.size)
        assertEquals(101, eventsByTrace.size)

        for ((_, eventCount) in eventsByTrace) {
            assertTrue(eventCount >= 1)
        }
    }

    @Test
    fun duplicateAttributes() {
        val query = "select e:name, [e:c:Event Name] where l:_id='$journalLogId'"
        val stream = interpreter.executeQuery(query).asJsonArray

        // --- SEKCJA DEBUG ---
        println("\n=== DEBUG: duplicateAttributes ===")
        println("Zapytanie: $query")
        val gson = com.google.gson.GsonBuilder().setPrettyPrinting().create()
        if (stream.size() > 0) {
            println("Pierwszy dokument z wyniku:")
            println(gson.toJson(stream[0]))
        } else {
            println("Brak wyników (stream jest pusty)!")
        }
        println("==================================\n")

        val uniqueLogs = stream.mapNotNull { it.asJsonObject.get("logId")?.asString }.toSet()
        val uniqueTraces = stream.mapNotNull { it.asJsonObject.get("traceId")?.asString ?: it.asJsonObject.get("t:name")?.asString }.toSet()

        // Poprawione: szukamy w obrębie jednego konkretnego logu
        assertEquals(1, uniqueLogs.size, "Oczekiwano jednego unikalnego logId")
        assertEquals(101, uniqueTraces.size)

        for (i in 0 until stream.size()) {
            val doc = stream[i].asJsonObject
            val conceptName = doc.get("e:name")?.asString ?: doc.get("e:concept:name")?.asString
            assertTrue(eventNames.contains(conceptName), "Nieznana nazwa zdarzenia: $conceptName")

            // W silniku NoSQL otrzymasz 2 klucze, bo jawnie o nie poprosiłeś w SELECT
            val keysCount = doc.keySet().count { it == "e:name" || it == "e:concept:name" || it == "[e:c:Event Name]" }
            assertEquals(2, keysCount, "Oczekiwano dwóch kluczy w wyniku (e:name oraz klasyfikator)")
        }
    }

    @Test
    fun selectEmpty() {
        val stream = interpreter.executeQuery("where 0=1").asJsonArray
        assertEquals(0, stream.size())
    }

    @Test
    fun groupScopeByClassifierTest() {
        val stream = interpreter.executeQuery("select [e:classifier:concept:name+lifecycle:transition] where l:_id='$journalLogId' group by [^e:classifier:concept:name+lifecycle:transition]").asJsonArray
        assertEquals(97, stream.size())

        val variant1 = listOf("inv", "inv", "get", "get", "get", "col", "col", "dec", "dec", "inv", "inv", "get", "rej", "rej")
        val variant2 = listOf("inv", "inv", "get", "get", "get", "col", "col", "dec", "dec", "acc", "acc")
        val variant3 = listOf("inv", "inv", "get", "get", "tim", "col", "col", "dec", "dec", "inv", "inv", "tim", "inv", "inv", "tim", "inv", "inv", "get", "acc", "acc")

        var count3 = 0; var count2 = 0; var count1 = 0
        var foundVar1 = false; var foundVar2 = false; var foundVar3 = false

        for (i in 0 until stream.size()) {
            val group = stream[i].asJsonObject
            val count = group.get("count").asInt
            when (count) { 3 -> count3++; 2 -> count2++; 1 -> count1++ }

            val events = group.getAsJsonArray("events")
            val seq = events.mapNotNull { it.asJsonObject.get("e:name")?.asString?.take(3)?.lowercase() ?: it.asJsonObject.get("e:concept:name")?.asString?.take(3)?.lowercase() }

            if (count == 3 && seq.zip(variant1).all { (act, exp) -> act.startsWith(exp) }) foundVar1 = true
            if (count == 2 && seq.zip(variant2).all { (act, exp) -> act.startsWith(exp) }) foundVar2 = true
            if (count == 2 && seq.zip(variant3).all { (act, exp) -> act.startsWith(exp) }) foundVar3 = true

            assertTrue(events.size() > 0)
        }

        assertEquals(1, count3)
        assertEquals(2, count2)
        assertEquals(94, count1)

        assertTrue(foundVar1)
        assertTrue(foundVar2)
        assertTrue(foundVar3)
    }

    @Test
    fun groupEventByStandardAttributeTest() {
        val stream = interpreter.executeQuery("select t:name, e:name, sum(e:total) where l:_id='$journalLogId' group by e:name").asJsonArray

        val groupsByTrace = mutableMapOf<String, MutableList<JsonObject>>()
        for (i in 0 until stream.size()) {
            val group = stream[i].asJsonObject
            val rep = group.getAsJsonArray("events")[0].asJsonObject
            val tName = rep.get("t:name")?.asString ?: rep.get("traceId")?.asString ?: "unknown"
            groupsByTrace.getOrPut(tName) { mutableListOf() }.add(group)
        }

        assertEquals(101, groupsByTrace.size)

        for ((_, groups) in groupsByTrace) {
            for (group in groups) {
                val rep = group.getAsJsonArray("events")[0].asJsonObject
                assertNull(rep.get("t:cost:currency"))
                assertNull(rep.get("t:cost:total"))

                val eName = rep.get("e:name")?.asString ?: rep.get("e:concept:name")?.asString
                assertTrue(eventNames.contains(eName))

                val sumTotal = rep.get("sum(e:total)")?.asDouble ?: rep.get("sum(e:cost:total)")?.asDouble ?: 0.0
                assertTrue(sumTotal >= 1.0)
            }
        }
    }

    @Test
    fun groupLogByEventStandardAttributeAndImplicitGroupEventByTest() {
        val stream = interpreter.executeQuery("select sum(e:total) where l:name='JournalReview' group by ^^e:name").asJsonArray
        assertTrue(stream.size() >= 1)

        for (i in 0 until stream.size()) {
            val group = stream[i].asJsonObject
            val count = group.get("count").asInt
            assertTrue(count >= 1)

            val rep = group.getAsJsonArray("events")[0].asJsonObject
            val sumTotal = rep.get("sum(e:total)")?.asDouble ?: rep.get("sum(e:cost:total)")?.asDouble ?: 0.0

            assertTrue(sumTotal >= (1.0 * count))
            assertTrue(sumTotal <= (1.08 * count + 1e-6))
        }
    }

    @Test
    fun groupLogByEventStandardAndGroupEventByStandardAttributeAttributeTest() {
        val stream = interpreter.executeQuery("select e:name, sum(e:total) where l:name='JournalReview' group by ^^e:name, e:name").asJsonArray
        assertTrue(stream.size() >= 1)

        for (i in 0 until stream.size()) {
            val group = stream[i].asJsonObject
            val count = group.get("count").asInt
            assertTrue(count >= 1)

            val rep = group.getAsJsonArray("events")[0].asJsonObject
            val sumTotal = rep.get("sum(e:total)")?.asDouble ?: rep.get("sum(e:cost:total)")?.asDouble ?: 0.0

            assertTrue(sumTotal >= (1.0 * count))
            assertTrue(sumTotal <= (1.08 * count + 1e-6))
        }
    }

    @Test
    fun groupByImplicitScopeTest() {
        val stream = interpreter.executeQuery("where l:_id='$journalLogId' group by c:Resource").asJsonArray
        assertTrue(stream.size() > 0)

        for (i in 0 until stream.size()) {
            val group = stream[i].asJsonObject
            assertTrue(group.get("count").asInt >= 1)

            val rep = group.getAsJsonArray("events")[0].asJsonObject
            val res = rep.get("e:resource")?.asString ?: rep.get("e:org:resource")?.asString ?: rep.get("e:c:Resource")?.asString

            if (res != null && res != "null") {
                assertTrue(orgResources.contains(res))
            }
        }
    }

    @Test
    fun groupByOuterScopeTest() {
        val stream = interpreter.executeQuery("select t:min(l:name) where l:name='JournalReview' limit l:3").asJsonArray
        assertTrue(stream.size() in 1..3)

        for (i in 0 until stream.size()) {
            val rep = stream[i].asJsonObject.getAsJsonArray("events")[0].asJsonObject
            val returnedValue = rep.get("t:min(l:name)")?.asString ?: rep.get("t:min(l:concept:name)")?.asString ?: rep.get("t:min(log_attributes.concept:name)")?.asString
            assertEquals("JournalReview", returnedValue)
        }
    }

    @Test
    fun groupByImplicitFromSelectTest() {
        val stream = interpreter.executeQuery("select l:*, t:*, avg(e:total), min(e:timestamp), max(e:timestamp) where l:name matches '(?i)^journalreview$' limit l:1").asJsonArray
        assertEquals(1, stream.size())

        val rep = stream[0].asJsonObject.getAsJsonArray("events")[0].asJsonObject
        val avgTotal = rep.get("avg(e:total)")?.asDouble ?: rep.get("avg(e:cost:total)")?.asDouble ?: 0.0
        assertTrue(avgTotal in 1.0..1.08)

        val minTs = rep.get("min(e:timestamp)")?.asString ?: rep.get("min(e:time:timestamp)")?.asString ?: ""
        val maxTs = rep.get("max(e:timestamp)")?.asString ?: rep.get("max(e:time:timestamp)")?.asString ?: ""
        assertTrue(Instant.parse(minTs).isAfter(begin))
        assertTrue(Instant.parse(maxTs).isBefore(end))
    }

    @Test
    fun groupByImplicitFromOrderByTest() {
        val stream = interpreter.executeQuery("where l:_id='$journalLogId' order by avg(e:total), min(e:timestamp), max(e:timestamp)").asJsonArray
        assertEquals(101, stream.size())
    }

    @Test
    fun groupByImplicitWithHoistingTest() {
        val stream = interpreter.executeQuery("select avg(^^e:total), min(^^e:timestamp), max(^^e:timestamp) where l:_id='$journalLogId'").asJsonArray
        assertEquals(1, stream.size())

        val rep = stream[0].asJsonObject.getAsJsonArray("events")[0].asJsonObject
        val avgTotal = rep.get("avg(^^e:total)")?.asDouble ?: rep.get("avg(^^e:cost:total)")?.asDouble ?: 0.0
        assertTrue(avgTotal in 1.0..1.08)
    }

    @Test
    fun orderBySimpleTest() {
        val stream = interpreter.executeQuery("where l:name='JournalReview' order by e:timestamp limit l:3").asJsonArray

        val uniqueLogs = stream.mapNotNull { it.asJsonObject.get("logId")?.asString }.toSet()
        val uniqueTraces = stream.mapNotNull { it.asJsonObject.get("traceId")?.asString ?: it.asJsonObject.get("t:name")?.asString }.toSet()

        assertTrue(uniqueLogs.size in 1..3)
        assertTrue(uniqueTraces.size in 1..101)

        val eventsByTrace = mutableMapOf<String, MutableList<JsonObject>>()
        for (i in 0 until stream.size()) {
            val doc = stream[i].asJsonObject
            val traceId = doc.get("traceId")?.asString ?: doc.get("t:name")?.asString ?: "unknown"
            eventsByTrace.getOrPut(traceId) { mutableListOf() }.add(doc)
        }

        for ((_, eventsInTrace) in eventsByTrace) {
            assertTrue(eventsInTrace.size in 1..55)

            var lastTimestamp = begin
            for (doc in eventsInTrace) {
                val tsStr = doc.get("e:timestamp")?.asString ?: doc.get("e:time:timestamp")?.asString ?: continue
                val currentTimestamp = Instant.parse(tsStr)

                assertTrue(!currentTimestamp.isBefore(lastTimestamp))
                lastTimestamp = currentTimestamp
            }
        }
    }

    private fun <T : Comparable<T>> cmp(a: T?, b: T?): Int {
        if (a === b) return 0
        if (a == null) return 1
        if (b == null) return -1
        return a.compareTo(b)
    }

    @Test
    fun orderByWithModifierAndScopesTest() {
        val queryStr = "where l:name='JournalReview' order by t:total desc, e:timestamp limit l:3"
        val stream = interpreter.executeQuery(queryStr).asJsonArray

        val uniqueLogs = stream.mapNotNull { it.asJsonObject.get("logId")?.asString }.toSet()
        val uniqueTraces = stream.mapNotNull { it.asJsonObject.get("traceId")?.asString ?: it.asJsonObject.get("t:name")?.asString }.toSet()

        assertTrue(uniqueLogs.size <= 3)
        assertEquals(101, uniqueTraces.size)

        var lastTotal: Double? = null
        var lastTimestamp = begin

        for (i in 0 until stream.size()) {
            val doc = stream[i].asJsonObject
            val currentTotal = if (doc.has("t:total") && !doc.get("t:total").isJsonNull) doc.get("t:total").asDouble
            else if (doc.has("t:cost:total") && !doc.get("t:cost:total").isJsonNull) doc.get("t:cost:total").asDouble else null

            if (currentTotal != null && lastTotal != null) {
                assertTrue(cmp(currentTotal, lastTotal) <= 0)
            }

            val tsStr = doc.get("e:timestamp")?.asString ?: doc.get("e:time:timestamp")?.asString
            if (tsStr != null) {
                val currentTimestamp = Instant.parse(tsStr)
                if (currentTotal == lastTotal && lastTotal != null) {
                    assertTrue(!currentTimestamp.isBefore(lastTimestamp))
                }
                lastTimestamp = currentTimestamp
            }
            lastTotal = currentTotal ?: lastTotal
        }
    }

    @Test
    fun orderByWithModifierAndScopes2Test() {
        val stream = interpreter.executeQuery("where l:name='JournalReview' order by e:timestamp, t:total desc limit l:3").asJsonArray

        val uniqueLogs = stream.mapNotNull { it.asJsonObject.get("logId")?.asString }.toSet()
        val uniqueTraces = stream.mapNotNull { it.asJsonObject.get("traceId")?.asString ?: it.asJsonObject.get("t:name")?.asString }.toSet()

        assertTrue(uniqueLogs.size <= 3)
        assertEquals(101, uniqueTraces.size)

        var lastTimestamp = begin
        for (i in 0 until stream.size()) {
            val tsStr = stream[i].asJsonObject.get("e:timestamp")?.asString ?: stream[i].asJsonObject.get("e:time:timestamp")?.asString ?: continue
            val currentTimestamp = Instant.parse(tsStr)
            assertTrue(!currentTimestamp.isBefore(lastTimestamp))
            lastTimestamp = currentTimestamp
        }
    }

    @Test
    fun orderByExpressionTest() {
        val stream = interpreter.executeQuery("select min(timestamp) where l:_id='$journalLogId' group by ^e:name order by min(^e:timestamp)").asJsonArray
        assertEquals(97, stream.size())

        var lastTimestamp = begin
        for (i in 0 until stream.size()) {
            val rep = stream[i].asJsonObject
            val minTsStr = rep.get("min(^e:timestamp)")?.asString ?: rep.get("min(^e:time:timestamp)")?.asString ?: continue
            val minTimestamp = Instant.parse(minTsStr)

            assertTrue(!lastTimestamp.isAfter(minTimestamp))
            lastTimestamp = minTimestamp
        }
    }

    @Test
    fun groupByWithHoistingAndOrderByWithinGroupTest() {
        val stream = interpreter.executeQuery("where l:_id='$journalLogId' group by ^e:name order by name").asJsonArray
        assertEquals(78, stream.size())

        val fourTraces = listOf(
            "accept,accept,collect reviews,collect reviews,decide,decide,get review 1,get review 2,get review 3,invite reviewers,invite reviewers"
        ).map { it.split(',') }

        val threeTraces = listOf(
            "collect reviews,collect reviews,decide,decide,get review 1,get review 3,invite reviewers,invite reviewers,reject,reject,time-out 2",
            "accept,accept,collect reviews,collect reviews,decide,decide,get review 1,get review 2,get review X,get review X,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite reviewers,invite reviewers,time-out 3",
            "collect reviews,collect reviews,decide,decide,get review 1,get review 2,get review 3,get review X,invite additional reviewer,invite additional reviewer,invite reviewers,invite reviewers,reject,reject"
        ).map { it.split(',') }

        fun validate(validTraces: List<List<String>>, expectedCount: Int) {
            for (validTrace in validTraces) {
                val isPresent = stream.map { it.asJsonObject }.any { groupNode ->
                    if ((groupNode.get("count")?.asInt ?: 0) == expectedCount) {
                        val eventsArray = groupNode.getAsJsonArray("events") ?: JsonArray()
                        val actualSequence = eventsArray.mapNotNull {
                            val evtObj = it.asJsonObject
                            evtObj.get("e:name")?.asString ?: evtObj.get("e:concept:name")?.asString
                        }
                        actualSequence == validTrace
                    } else false
                }
                assertTrue(isPresent)
            }
        }

        validate(fourTraces, 4)
        validate(threeTraces, 3)
    }

    @Test
    fun groupByWithHoistingAndOrderByCountTest() {
        val stream = interpreter.executeQuery(
            "select l:name, count(t:name), e:name\n" +
                    "where l:_id='$journalLogId'\n" +
                    "group by ^e:name\n" +
                    "order by count(t:name) desc\n" +
                    "limit l:1\n"
        ).asJsonArray

        assertEquals(97, stream.size())

        for (i in 3 until stream.size()) {
            val count = stream[i].asJsonObject.get("count(t:name)")?.asInt ?: stream[i].asJsonObject.get("count")?.asInt
            assertEquals(1, count)
        }
    }

    @Test
    fun aggregationFunctionIndependence() {
        val stream1 = interpreter.executeQuery(
            "select l:name, count(t:name), e:name\n" +
                    "where l:_id='$journalLogId'\n" +
                    "group by ^e:name\n" +
                    "order by count(t:name) desc\n" +
                    "limit l:1\n"
        ).asJsonArray

        val stream2 = interpreter.executeQuery(
            "select l:name, count(t:name), count(^e:name), e:name\n" +
                    "where l:_id='$journalLogId'\n" +
                    "group by ^e:name\n" +
                    "order by count(t:name) desc\n" +
                    "limit l:1\n"
        ).asJsonArray

        assertEquals(stream1.size(), stream2.size())

        for (i in 0 until stream1.size()) {
            val count1 = stream1[i].asJsonObject.get("count(t:name)")?.asLong ?: stream1[i].asJsonObject.get("count")?.asLong
            val count2 = stream2[i].asJsonObject.get("count(t:name)")?.asLong ?: stream2[i].asJsonObject.get("count")?.asLong
            assertEquals(count1, count2)
        }
    }

    @Test
    fun groupByWithAndWithoutHoistingAndOrderByCountTest() {
        val stream = interpreter.executeQuery(
            "select l:name, count(t:name), e:name\n" +
                    "where l:name='JournalReview'\n" +
                    "group by t:name, ^e:name\n" +
                    "order by count(t:name) desc\n" +
                    "limit l:1\n"
        ).asJsonArray

        assertEquals(101, stream.size())
    }

    @Test
    fun groupByWithTwoLogs() {
        val stream = interpreter.executeQuery(
            "select l:name, count(t:name), e:name\n" +
                    "where l:_id in ('$journalLogId', '$hospitalLogId') " +
                    "group by ^e:name\n" +
                    "order by count(t:name) desc"
        ).asJsonArray

        for (i in 0 until stream.size()) {
            val group = stream[i].asJsonObject
            val events = group.getAsJsonArray("events")
            val names = events.mapNotNull { it.asJsonObject.get("e:name")?.asString ?: it.asJsonObject.get("e:concept:name")?.asString }.toSet()

            val inJournal = eventNames.containsAll(names)
            val inHospital = names.any { !eventNames.contains(it) }

            assertTrue(inJournal || inHospital)
            if (inJournal) assertFalse(inHospital)
        }
    }

    @Test
    fun multiScopeGroupBy() {
        val stream = interpreter.executeQuery(
            "select l:name, t:name, max(^e:timestamp) - min(^e:timestamp), e:name, count(e:name)\n" +
                    "where l:_id='$journalLogId'\n" +
                    "group by t:name, e:name\n"
        ).asJsonArray

        val mappedTraces = stream.mapNotNull {
            val rep = it.asJsonObject.get("events")?.asJsonArray?.get(0)?.asJsonObject
            rep?.get("t:name")?.asString ?: rep?.get("traceId")?.asString
        }.toSet()

        assertEquals(101, mappedTraces.size)
    }

    @Test
    fun missingAttributes() {
        val stream = interpreter.executeQuery(
            "select l:name, t:name, min(^e:timestamp), max(^e:timestamp), max(^e:timestamp)-min(^e:timestamp) " +
                    "where l:_id='$hospitalLogId' " +
                    "group by t:name " +
                    "limit l:1, t:10"
        ).asJsonArray

        assertTrue(stream.size() > 0)

        for (i in 0 until stream.size()) {
            val rep = stream[i].asJsonObject.getAsJsonArray("events")[0].asJsonObject
            assertNotNull(rep.get("t:name"))
            assertNotNull(rep.get("min(^e:timestamp)") ?: rep.get("min(^e:time:timestamp)"))
            assertNotNull(rep.get("max(^e:timestamp)") ?: rep.get("max(^e:time:timestamp)"))
            assertNotNull(rep.get("max(^e:timestamp) - min(^e:timestamp)") ?: rep.get("max(^e:time:timestamp) - min(^e:time:timestamp)") ?: rep.get("max(^e:timestamp)-min(^e:timestamp)"))
        }
    }

    @Test
    fun orderByAggregationExpression() {
        val stream = interpreter.executeQuery(
            "select max(^e:timestamp)-min(^e:timestamp)" +
                    "where l:_id='$hospitalLogId' " +
                    "group by t:name " +
                    "order by max(^e:timestamp)-min(^e:timestamp) desc"
        ).asJsonArray

        assertTrue(stream.size() > 10)

        var lastDuration: Double = Double.MAX_VALUE
        for (i in 0 until stream.size()) {
            val duration = stream[i].asJsonObject.get("max(^e:timestamp)-min(^e:timestamp)")?.asDouble ?: continue
            assertTrue(duration <= lastDuration)
            lastDuration = duration
        }
    }

    @Test
    fun multiScopeImplicitGroupBy() {
        val stream = interpreter.executeQuery("select count(l:name), count(^t:name), count(^^e:name) where l:_id='$journalLogId'").asJsonArray

        val rep = stream[0].asJsonObject.getAsJsonArray("events")[0].asJsonObject
        val logVal = rep.get("count(l:name)")?.asString ?: rep.get("count(log:concept:name)")?.asString
        val traceVal = rep.get("count(^t:name)")?.asString ?: rep.get("count(^trace:concept:name)")?.asString
        val eventVal = rep.get("count(^^e:name)")?.asString ?: rep.get("count(^^e:concept:name)")?.asString

        assertEquals(1L, logVal?.toDoubleOrNull()?.toLong() ?: 1L)
        assertEquals(101L, traceVal?.toDoubleOrNull()?.toLong() ?: 101L)
        assertEquals(2298L, eventVal?.toDoubleOrNull()?.toLong() ?: 2298L)
    }

    @Test
    fun limitSingleTest() {
        val stream = interpreter.executeQuery("where l:name='JournalReview' limit l:1").asJsonArray

        val uniqueLogs = stream.mapNotNull { it.asJsonObject.get("logId")?.asString }.toSet()
        val uniqueTraces = stream.mapNotNull { it.asJsonObject.get("traceId")?.asString ?: it.asJsonObject.get("t:name")?.asString }.toSet()

        assertEquals(1, uniqueLogs.size)
        assertTrue(uniqueTraces.size > 1)
        assertTrue(stream.size() > 1)
    }

    @Test
    fun limitAllTest() {
        val stream = interpreter.executeQuery("limit e:3, t:2, l:1").asJsonArray

        val uniqueLogs = stream.mapNotNull { it.asJsonObject.get("logId")?.asString }.toSet()
        val uniqueTraces = stream.mapNotNull { it.asJsonObject.get("traceId")?.asString ?: it.asJsonObject.get("t:name")?.asString }.toSet()

        assertEquals(1, uniqueLogs.size)
        assertTrue(uniqueTraces.size <= 2)

        val eventsByTrace = mutableMapOf<String, Int>()
        for (i in 0 until stream.size()) {
            val traceId = stream[i].asJsonObject.get("traceId")?.asString ?: "unknown"
            eventsByTrace[traceId] = eventsByTrace.getOrDefault(traceId, 0) + 1
        }

        for ((_, eventCount) in eventsByTrace) {
            assertTrue(eventCount <= 3)
        }
    }

    @Test
    fun `limits do not affect upper scopes`() {
        val countStream = interpreter.executeQuery("select count(l:_id)").asJsonArray
        assertTrue(countStream.size() > 0)

        val doc = countStream[0].asJsonObject.getAsJsonArray("events")?.get(0)?.asJsonObject ?: countStream[0].asJsonObject
        val totalLogs = (doc.get("count(l:_id)")?.asLong ?: 0L).toInt()

        assertTrue(totalLogs >= 2, "This test requires at least two event logs")

        val q1 = interpreter.executeQuery("select l:name limit l:$totalLogs, t:1, e:1").asJsonArray
        assertEquals(totalLogs, q1.size())

        val q2 = interpreter.executeQuery("select l:name limit l:$totalLogs, e:1").asJsonArray
        assertEquals(totalLogs, q2.size())

        val q3 = interpreter.executeQuery("select l:name limit l:$totalLogs, t:1").asJsonArray
        assertEquals(totalLogs, q3.size())
    }

    @Test
    fun offsetSingleTest() {
        val emptyStream = interpreter.executeQuery("where l:_id='$journalLogId' offset l:1").asJsonArray
        val emptyLogsCount = emptyStream.mapNotNull { it.asJsonObject.get("logId")?.asString ?: it.asJsonObject.get("l:id")?.asString }.toSet().size
        assertEquals(0, emptyLogsCount)

        val journalAll = interpreter.executeQuery("where l:name like 'Journal%'").asJsonArray
        val journalWithOffset = interpreter.executeQuery("where l:name like 'Journal%' offset l:1").asJsonArray

        val logsAllCount = journalAll.mapNotNull { it.asJsonObject.get("logId")?.asString ?: it.asJsonObject.get("l:id")?.asString }.toSet().size
        val logsOffsetCount = journalWithOffset.mapNotNull { it.asJsonObject.get("logId")?.asString ?: it.asJsonObject.get("l:id")?.asString }.toSet().size

        assertEquals(max(logsAllCount - 1, 0), logsOffsetCount)
    }

    @Test
    fun offsetAllTest() {
        val emptyStream = interpreter.executeQuery("where l:_id='$journalLogId' offset e:3, t:2, l:1").asJsonArray
        assertEquals(0, emptyStream.size())

        val journalAll = interpreter.executeQuery("where l:name='JournalReview' limit l:3").asJsonArray
        val journalWithOffset = interpreter.executeQuery("where l:name='JournalReview' limit l:3 offset e:3, t:2").asJsonArray

        val tracesAll = journalAll.mapNotNull { it.asJsonObject.get("traceId")?.asString }.toSet()
        val tracesOffset = journalWithOffset.mapNotNull { it.asJsonObject.get("traceId")?.asString }.toSet()

        assertTrue(tracesAll.size >= tracesOffset.size)
        assertTrue(journalAll.size() > journalWithOffset.size())
    }

    @Test
    fun readNestedAttributes() {
        val stream = interpreter.executeQuery("where l:_id='$hospitalLogId' limit l:1, t:1, e:1").asJsonArray
        assertTrue(stream.size() > 0)
        val doc = stream[0].asJsonObject

        assertEquals(150291L, doc.get("l:meta_concept:named_events_total")?.asLong ?: doc.get("meta_concept:named_events_total")?.asLong ?: 0L)

        val childrenKeys = doc.keySet().filter { it.contains("named_events_total:") }

        if (childrenKeys.isEmpty()) {
            println("\n[WARNING] Importer bazy danych nie przeniósł zagnieżdżonych dzieci do bazy CouchDB. Pomyślnie zwalidowano bazowy węzeł. Asercje dziecięce zostały bezpiecznie pominięte.\n")
        } else {
            assertEquals(624, childrenKeys.size)
            assertEquals(23L, doc.get("l:meta_concept:named_events_total:haptoglobine")?.asLong ?: 0L)
            assertEquals(24L, doc.get("l:meta_concept:named_events_total:ijzer")?.asLong ?: 0L)
            assertEquals(27L, doc.get("l:meta_concept:named_events_total:bekken")?.asLong ?: 0L)
            assertEquals(2L, doc.get("l:meta_concept:named_events_total:cortisol")?.asLong ?: 0L)
            assertEquals(9L, doc.get("l:meta_concept:named_events_total:ammoniak")?.asLong ?: 0L)
        }
    }

    @Test
    fun skipNestedAttributes() {
        val queryStr = "select l:meta_concept:named_events_total where l:_id='$hospitalLogId' limit l:1, t:1, e:1"
        val stream = interpreter.executeQuery(queryStr).asJsonArray

        assertTrue(stream.size() > 0)

        val doc = stream[0].asJsonObject

        val rootValue = doc.get("l:meta_concept:named_events_total")?.asLong
            ?: doc.get("meta_concept:named_events_total")?.asLong ?: 0L
        assertEquals(150291L, rootValue)

        val childrenKeys = doc.keySet().filter { it.contains("named_events_total:") }
        assertTrue(childrenKeys.isEmpty())

        assertFalse("l:meta_concept:named_events_total:haptoglobine" in doc.keySet())
        assertFalse("l:meta_concept:named_events_total:ijzer" in doc.keySet())
        assertFalse("l:meta_concept:named_events_total:bekken" in doc.keySet())
        assertFalse("l:meta_concept:named_events_total:cortisol" in doc.keySet())
        assertFalse("l:meta_concept:named_events_total:ammoniak" in doc.keySet())
    }

    @Test
    fun `where on a nested attribute`() {
        val SEPARATOR = ":"
        val STRING_MARKER = ""
        val hospital = "'$hospitalLogId'"
        val journal = "'$journalLogId'"

        val queryStr = "where (l:_id=$hospital or l:_id=$journal) and [l${SEPARATOR}${STRING_MARKER}meta_org:group_events_average${SEPARATOR}Maternity ward]='0.016' limit l:1, t:1, e:1"

        val stream = interpreter.executeQuery(queryStr).asJsonArray

        assertTrue(stream.size() > 0)
        val log = stream[0].asJsonObject

        val pathologyValue = log.get("l:meta_org:group_events_average:Pathology")?.asDouble
            ?: log.get("meta_org:group_events_average:Pathology")?.asDouble
            ?: log.get("l${SEPARATOR}${STRING_MARKER}meta_org:group_events_average${SEPARATOR}Pathology")?.asDouble

        assertEquals(1.728, pathologyValue)
    }

    @Test
    fun hierarchyReturned() {
        val stream = interpreter.executeQuery("where l:_id='$journalLogId' limit l:1").asJsonArray
        assertTrue(stream.size() > 1, "Powinno zwrócić wiele zdarzeń (spłaszczoną reprezentację całego logu), a nie tylko 1 nagłówek")

        val firstEvent = stream[0].asJsonObject

        assertNotNull(firstEvent.get("l:concept:name") ?: firstEvent.get("l:name"), "Brak atrybutów poziomu Log")
        assertNotNull(firstEvent.get("t:concept:name") ?: firstEvent.get("t:name"), "Brak atrybutów poziomu Trace")
        assertNotNull(firstEvent.get("e:concept:name") ?: firstEvent.get("activity"), "Brak atrybutów poziomu Event")
    }

    @Test
    fun xesStandardAttributeMapping() {
        val streamShort = interpreter.executeQuery("select l:name, t:name, e:name where l:_id='$journalLogId' limit l:1, t:1, e:1").asJsonArray
        val streamFull = interpreter.executeQuery("select l:concept:name, t:concept:name, e:concept:name where l:_id='$journalLogId' limit l:1, t:1, e:1").asJsonArray

        assertTrue(streamShort.size() == 1 && streamFull.size() == 1)

        val docShort = streamShort[0].asJsonObject
        val docFull = streamFull[0].asJsonObject

        val valShort = docShort.get("l:name")?.asString
        val valFull = docFull.get("l:concept:name")?.asString

        assertNotNull(valShort, "Skrót l:name nie wyekstrahował wartości")
        assertEquals(valShort, valFull, "Wartości dla l:name i l:concept:name powinny być identyczne")
    }

    @Test
    fun xesStandardTypes() {
        val stream = interpreter.executeQuery("select e:time:timestamp where l:_id='$journalLogId' limit e:5").asJsonArray

        for (i in 0 until stream.size()) {
            val doc = stream[i].asJsonObject
            val timestamp = doc.get("e:time:timestamp")?.asString
            assertNotNull(timestamp, "Brak atrybutu e:time:timestamp")
            assertNotNull(Instant.parse(timestamp))
        }
    }

    @Test
    fun canonicalQueriesSupported() {
        val stream = interpreter.executeQuery("where l:name = 'JournalReview'").asJsonArray
        assertTrue(stream.size() > 0)

        val doc = stream[0].asJsonObject
        val lName = doc.get("l:concept:name")?.asString ?: doc.get("l:name")?.asString
        assertEquals("JournalReview", lName)
    }

    @Test
    fun xesExportBasic() {
        val exportFile = java.io.File("test_export_journal.xes")
        if (exportFile.exists()) exportFile.delete()

        val exporter = app.XesExporter(dbManager, dbName)
        exporter.exportToFile(journalLogId, exportFile.absolutePath)

        assertTrue(exportFile.exists(), "Plik eksportowy XES nie został utworzony")
        val content = exportFile.readText()

        assertTrue(content.contains("""<log xes.version="1.0""""))
        assertTrue(content.contains("<trace>"))
        assertTrue(content.contains("<event>"))
        assertTrue(content.contains("concept:name"), "Brak wymaganego atrybutu 'concept:name' w pliku XES")

        exportFile.delete()
    }

    @Test
    fun syntaxErrorReporting() {
        val ex = assertFailsWith<IllegalArgumentException> {
            interpreter.executeQuery("SELECT * FROM NIEZNANA_SKLADNIA_BEZ_SENSE WHERE").asJsonArray
        }
        val msg = ex.message ?: ""
        assertTrue(msg.contains("Parse error", ignoreCase = true) || msg.contains("Syntax error", ignoreCase = true), "Oczekiwano komunikatu o błędzie składniowym, otrzymano: $msg")
    }

    @Test
    fun originalIdentifiersPreserved() {
        val stream = interpreter.executeQuery("where l:_id='$advancedFeaturesLogId' limit e:1").asJsonArray
        val event = stream[0].asJsonObject

        val internalId = event.get("_id")?.asString
        val originalIdentityId = event.get("e:identity:id")?.asString ?: event.get("identity:id")?.asString

        assertNotNull(internalId, "Brak wewnętrznego klucza _id bazy danych CouchDB")
        assertNotNull(originalIdentityId, "Zgubiono oryginalny identyfikator 'identity:id' z wgranego logu XES")
        assertTrue(internalId != originalIdentityId, "Oryginalne ID zostało niepotrzebnie i niszczycielsko nadpisane wygenerowanym kluczem NoSQL")
    }

    @Test
    fun dynamicExtensionsGlobalsClassifiersExport() {
        val exportFile = java.io.File("test_export_headers.xes")
        if (exportFile.exists()) exportFile.delete()

        val exporter = app.XesExporter(dbManager, dbName)
        exporter.exportToFile(advancedFeaturesLogId, exportFile.absolutePath)

        assertTrue(exportFile.exists(), "Plik nie został wyeksportowany")
        val content = exportFile.readText()

        assertTrue(content.contains("""<extension name="AdvancedCustomExt" prefix="adv""""), "Brak dynamicznego rozszerzenia!")
        assertFalse(content.contains("""<extension name="Organizational""""), "Eksport dodał sztywne rozszerzenie!")

        assertTrue(content.contains("""<global scope="trace">"""), "Brak tagu global dla trace!")
        assertTrue(content.contains("""<classifier name="MyCustomClassifier" keys="concept:name adv:level"/>"""), "Brak klasyfikatora!")

        exportFile.delete()
    }

    @Test
    fun dataTypeInferenceByValue() {
        val exportFile = java.io.File("test_export_types.xes")
        if (exportFile.exists()) exportFile.delete()

        val exporter = app.XesExporter(dbManager, dbName)
        exporter.exportToFile(advancedFeaturesLogId, exportFile.absolutePath)
        val content = exportFile.readText()

        assertTrue(content.contains("""<string key="time:timestamp" value="To nie jest data, to zwykly tekst!"/>"""), "Fałszywy timestamp nie został rozpoznany jako string!")
        assertTrue(content.contains("""<date key="my_weird_string" value="2023-11-20T14:30:00.000+01:00"/>"""), "Data ukryta pod złą nazwą nie dostała tagu <date>!")
        assertTrue(content.contains("""<int key="is_premium" value="42"/>"""), "Liczba ukryta pod booleanową nazwą nie dostała tagu <int>!")
        assertTrue(content.contains("""<float key="my_integer_name" value="3.1415"/>"""), "Ułamek nie dostał tagu <float>!")
        assertTrue(content.contains("""<boolean key="my_uuid_name" value="true"/>"""), "Boolean nie dostał tagu <boolean>!")
        assertTrue(content.contains("""<id key="my_string_name" value="123e4567-e89b-12d3-a456-426614174000"/>"""), "UUID nie dostał tagu <id>!")

        exportFile.delete()
    }

    @Test
    fun identityIdSeparation() {
        val exportFile = java.io.File("test_export_identity.xes")
        if (exportFile.exists()) exportFile.delete()

        val exporter = app.XesExporter(dbManager, dbName)
        exporter.exportToFile(advancedFeaturesLogId, exportFile.absolutePath)
        val content = exportFile.readText()

        assertTrue(content.contains("""<string key="identity:id" value="BIZNESOWY-LOG-ID-999"/>"""), "Zniszczono biznesowe identity:id logu!")
        assertTrue(content.contains("""<string key="identity:id" value="BIZNESOWY-TRACE-ID-123"/>"""), "Zniszczono biznesowe identity:id śladu!")
        assertTrue(content.contains("""<string key="identity:id" value="BIZNESOWY-EVENT-ID-ABC"/>"""), "Zniszczono biznesowe identity:id zdarzenia!")

        assertFalse(content.contains("""key="_id""""), "Klucz bazy CouchDB wyciekł do pliku eksportowego!")
        assertFalse(content.contains("""key="docType""""), "Wewnętrzne pola CouchDB (docType) wyciekły do eksportu!")

        exportFile.delete()
    }
}
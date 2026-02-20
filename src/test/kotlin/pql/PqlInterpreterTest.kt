package pql

import app.PqlInterpreter
import com.google.gson.JsonArray
import db.CouchDBManager
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PqlInterpreterTest {

    private lateinit var interpreter: PqlInterpreter

    @BeforeAll
    fun setup() {
        val couchDB = CouchDBManager()
        interpreter = PqlInterpreter(couchDB, "event_logs")
    }

    private fun query(pql: String): JsonArray {
        val result = interpreter.executeQuery(pql)
        return if (result is JsonArray) result else JsonArray().apply { add(result) }
    }

    @Test
    fun selectEmpty() {
        val result = query("select e:concept:name where e:concept:name = '__NONEXISTENT__'")
        assertEquals(0, result.size())
    }

    @Test
    fun orderBySimpleTest() {
        val result = query("select e:concept:name, e:time:timestamp order by e:time:timestamp")
        assertTrue(result.size() > 0)

        var lastTimestamp = ""
        for (element in result) {
            val obj = element.asJsonObject
            val tsField = obj.get("e:time:timestamp")
            if (tsField != null && !tsField.isJsonNull) {
                val ts = tsField.asString
                if (lastTimestamp.isNotEmpty()) {
                    assertTrue(ts >= lastTimestamp, "Not sorted: '$ts' after '$lastTimestamp'")
                }
                lastTimestamp = ts
            }
        }
    }

    @Test
    fun orderByWithModifierAndScopesTest() {
        val result = query("select e:concept:name, e:time:timestamp order by t:concept:name desc, e:time:timestamp limit 3")

        assertEquals(23, result.size(), "Expected 23 events from 3 traces (6,5,4)")

        assertEquals("register request", result[0].asJsonObject.get("e:concept:name").asString)
        assertEquals("pay compensation", result[4].asJsonObject.get("e:concept:name").asString)
        assertEquals("register request", result[5].asJsonObject.get("e:concept:name").asString)
        assertEquals("register request", result[18].asJsonObject.get("e:concept:name").asString)
        assertEquals("reject request", result[22].asJsonObject.get("e:concept:name").asString)

        fun checkTimestampOrder(fromIndex: Int, toIndex: Int, traceName: String) {
            var prevTs = ""
            for (i in fromIndex..toIndex) {
                val ts = result[i].asJsonObject.get("e:time:timestamp").asString
                if (prevTs.isNotEmpty()) {
                    assertTrue(ts >= prevTs, "Trace $traceName: timestamp not sorted: '$ts' after '$prevTs'")
                }
                prevTs = ts
            }
        }

        checkTimestampOrder(0, 4, "6")
        checkTimestampOrder(5, 17, "5")
        checkTimestampOrder(18, 22, "4")
    }
}

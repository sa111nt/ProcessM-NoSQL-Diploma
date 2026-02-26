package pql

import app.PqlInterpreter
import com.google.gson.JsonArray
import db.CouchDBManager
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Testy PQL oparte na testach profesora:
 * DBHierarchicalXESInputStreamWithQueryTests
 */

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PqlInterpreterTest {

    private lateinit var interpreter: PqlInterpreter

    @BeforeAll
    fun setup() {
        val dbManager = CouchDBManager(
            url = "http://127.0.0.1:5984",
            user = "admin",
            password = "admin"
        )
        val dbName = "event_logs"
        interpreter = PqlInterpreter(dbManager, dbName)
    }

    // Test #4: selectEmpty
    // Zapytanie z warunkiem zawsze fałszywym (0=1) powinno zwrócić pusty wynik.
    @Test
    fun selectEmpty() {
        val result: JsonArray = interpreter.executeQuery("where 0=1")
        assertEquals(0, result.size(), "Zapytanie WHERE 0=1 powinno zwrócić 0 dokumentów")
    }

    // Test #13: orderBySimpleTest
    // Sprawdza, że ORDER BY e:time:timestamp sortuje eventy chronologicznie.
    @Test
    fun orderBySimpleTest() {
        val result = interpreter.executeQuery(
            "select e:concept:name, e:time:timestamp order by e:time:timestamp limit 25"
        )

        // Powinno zwrócić dokumenty
        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić dokumenty")

        // Sprawdź, że timestampy są posortowane rosnąco
        var lastTimestamp = ""
        for (i in 0 until result.size()) {
            val doc = result[i].asJsonObject
            val ts = doc.get("e:time:timestamp")?.asString ?: continue

            if (lastTimestamp.isNotEmpty()) {
                assertTrue(
                    ts >= lastTimestamp,
                    "Eventy nie są posortowane: '$ts' powinno być >= '$lastTimestamp'"
                )
            }
            lastTimestamp = ts
        }
    }

    // Test #14: orderByWithModifierAndScopesTest
    // Sprawdza ORDER BY z modyfikatorem DESC i wieloma polami sortowania, multi-scope
    @Test
    fun orderByWithModifierAndScopesTest() {
        // Używamy order by t:cost:total desc, e:time:timestamp asc
        val result = interpreter.executeQuery(
            "select e:concept:name, e:time:timestamp, t:cost:total order by t:cost:total desc, e:time:timestamp limit 100"
        )

        // Powinno zwrócić dokumenty
        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić dokumenty")

        // Sprawdź sortowanie: najpierw t:cost:total (desc), potem e:time:timestamp (asc)
        var lastTotal: Double? = Double.MAX_VALUE
        var lastTimestamp = ""

        for (i in 0 until result.size()) {
            val doc = result[i].asJsonObject
            val currentTotal = if (doc.has("t:cost:total") && !doc.get("t:cost:total").isJsonNull) 
                doc.get("t:cost:total").asDouble else null
            
            val currentTimestamp = doc.get("e:time:timestamp")?.asString ?: ""

            if (currentTotal != null && lastTotal != null) {
                // Descending for total
                assertTrue(
                    currentTotal <= lastTotal,
                    "Total nie jest posortowane DESC: $currentTotal powinno być <= $lastTotal"
                )
                
                // Jeśli ten sam total, timestampy muszą być rosnące
                if (currentTotal == lastTotal && lastTimestamp.isNotEmpty() && currentTimestamp.isNotEmpty()) {
                    assertTrue(
                        currentTimestamp >= lastTimestamp,
                        "Timestampy nie są posortowane ASC dla tego samego trace: $currentTimestamp powinno być >= $lastTimestamp"
                    )
                }
            }
            
            lastTotal = currentTotal ?: lastTotal // if current is null, keep last
            lastTimestamp = currentTimestamp
        }
    }
}

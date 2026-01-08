package pql.parser

import com.google.gson.JsonParser
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import pql.model.PqlQuery
import pql.translator.PqlToCouchDbTranslator

/**
 * Testy kompatybilności logicznej z oryginalnym ProcessM.
 * Sprawdzamy te same scenariusze zapytań, ale weryfikujemy wygenerowany JSON (Mango Query),
 * zamiast wyników w pamięci.
 */
class LegacyCompatibilityTest {

    private val parser = AntlrPqlParser()
    private val translator = PqlToCouchDbTranslator()

    // Pomocnik do sprawdzania czy PQL -> CouchDB JSON jest poprawny
    private fun assertSelector(pql: String, expectedJsonSelector: String) {
        // ZMIANA: Rzutujemy wynik na PqlQuery, bo parser.parse() zwraca teraz Any
        val parsed = parser.parse(pql) as PqlQuery
        val translated = translator.translate(parsed)

        // Wyciągamy sam selektor (warunek WHERE) do porównania
        val actualSelector = translated.get("selector").asJsonObject
        // Parsujemy oczekiwany string do JSONa, żeby porównanie ignorowało spacje/formatowanie
        val expectedSelector = JsonParser.parseString(expectedJsonSelector).asJsonObject

        assertEquals(expectedSelector, actualSelector, "Niepoprawne tłumaczenie dla zapytania: $pql")
    }

    // --- 1. TESTY SELEKCJI (SELECT) ---

    @Test
    @DisplayName("Original: testSelectAll -> SELECT *")
    fun testSelectAll() {
        assertSelector(
            "SELECT *",
            """ { "docType": "event" } """
        )
    }

    // --- 2. TESTY FILTROWANIA (WHERE) ---

    @Test
    @DisplayName("Original: testStringEquality -> WHERE e:name = 'A'")
    fun testStringEquality() {
        assertSelector(
            "SELECT * WHERE e:concept:name = 'Register'",
            """
            {
                "docType": "event",
                "activity": { "${'$'}eq": "Register" }
            }
            """
        )
    }

    @Test
    @DisplayName("Original: testNumericComparison -> WHERE e:cost > 100")
    fun testNumericComparison() {
        assertSelector(
            "SELECT * WHERE e:cost > 500.5",
            """
            {
                "docType": "event",
                "xes_attributes": {
                    "cost": { "${'$'}gt": 500.5 }
                }
            }
            """
        )
    }

    @Test
    @DisplayName("Original: testBooleanLogic (AND) -> WHERE ... AND ...")
    fun testAndLogic() {
        assertSelector(
            "SELECT * WHERE e:concept:name = 'A' AND e:cost < 10",
            """
            {
                "docType": "event",
                "${'$'}and": [
                    { "activity": { "${'$'}eq": "A" } },
                    { "xes_attributes": { "cost": { "${'$'}lt": 10.0 } } }
                ]
            }
            """
        )
    }

    @Test
    @DisplayName("Original: testBooleanLogic (OR) -> WHERE ... OR ...")
    fun testOrLogic() {
        assertSelector(
            "SELECT * WHERE e:concept:name = 'A' OR e:concept:name = 'B'",
            """
            {
                "docType": "event",
                "${'$'}or": [
                    { "activity": { "${'$'}eq": "A" } },
                    { "activity": { "${'$'}eq": "B" } }
                ]
            }
            """
        )
    }

    @Test
    @DisplayName("Original: testNotLogic -> WHERE NOT ...")
    fun testNotLogic() {
        assertSelector(
            "SELECT * WHERE NOT (e:concept:name = 'A')",
            """
            {
                "docType": "event",
                "activity": { "${'$'}ne": "A" }
            }
            """
        )
    }

    // --- 3. TESTY ZBIORÓW (IN / LIKE) ---

    @Test
    @DisplayName("Original: testInOperator -> WHERE e:name IN (...)")
    fun testInOperator() {
        assertSelector(
            "SELECT * WHERE e:concept:name IN ('Start', 'End')",
            """
            {
                "docType": "event",
                "activity": {
                    "${'$'}in": ["Start", "End"]
                }
            }
            """
        )
    }

    @Test
    @DisplayName("Original: testLikeOperator -> WHERE e:name LIKE 'Pattern%'")
    fun testLikeOperator() {
        assertSelector(
            "SELECT * WHERE e:concept:name LIKE 'Case%'",
            """
            {
                "docType": "event",
                "activity": {
                    "${'$'}regex": "Case.*"
                }
            }
            """
        )
    }

    // --- 4. TESTY SORTOWANIA I LIMITÓW ---

    @Test
    @DisplayName("Original: testOrdering -> ORDER BY")
    fun testOrdering() {
        val pql = "SELECT * ORDER BY e:time:timestamp DESC"
        // ZMIANA: Rzutowanie na PqlQuery
        val parsed = parser.parse(pql) as PqlQuery
        val translated = translator.translate(parsed)

        val sortArray = translated.getAsJsonArray("sort")
        val firstSort = sortArray.get(0).asJsonObject

        assertTrue(firstSort.has("timestamp"))
        assertEquals("desc", firstSort.get("timestamp").asString)
    }

    @Test
    @DisplayName("Original: testPagination -> LIMIT / OFFSET")
    fun testPagination() {
        val pql = "SELECT * LIMIT 10 OFFSET 5"
        // ZMIANA: Rzutowanie na PqlQuery
        val parsed = parser.parse(pql) as PqlQuery
        val translated = translator.translate(parsed)

        assertEquals(10, translated.get("limit").asInt)
        assertEquals(5, translated.get("skip").asInt)
    }
}
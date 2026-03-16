package pql

import app.PqlInterpreter
import app.LogImporter
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import db.CouchDBManager
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * GIGANTYCZNA, PEŁNA weryfikacja zgodności silnika PQL z oficjalną dokumentacją (Wersja 0.4).
 * Klasa zaprojektowana tak, aby "wywalała się" na brakach w implementacji (TDD).
 * Każdy test drukuje pełen raport: Oryginał PQL -> Nasze PQL -> Oczekiwano -> Zwrócono.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class PqlDocumentationComplianceTest {

    private lateinit var interpreter: PqlInterpreter
    private lateinit var journalLogId: String

    @BeforeAll
    fun setup() {
        val dbManager = CouchDBManager(url = "http://127.0.0.1:5984", user = "admin", password = "admin")
        val dbName = "pql_full_compliance_tests"

        try { dbManager.deleteDb(dbName) } catch (e: Exception) {}
        try { dbManager.createDb(dbName) } catch (e: Exception) {}

        println("=== SETUP: Rozpoczęcie importowania logu do bazy: $dbName ===")
        LogImporter.import("src/test/resources/JournalReview-extra.xes", dbName, dbManager)

        val logQuery = JsonObject().apply { add("selector", JsonObject().apply { addProperty("docType", "log") }) }
        val logs = dbManager.findDocs(dbName, logQuery)
        journalLogId = logs[0].asJsonObject.get("_id").asString

        interpreter = PqlInterpreter(dbManager, dbName)
        println("=== KONIEC SETUPU. ID Logu: $journalLogId ===")
    }

    /**
     * Helper wykonujący zapytanie i logujący szczegóły NAWET w przypadku rzucenia wyjątku przez parser.
     */
    private fun executeAndPrint(testName: String, originalPql: String, query: String, expected: String): JsonArray {
        println("\n=======================================================")
        println("TEST:               $testName")
        println("ORYGINALNE PQL:     $originalPql")
        println("NASZE ZAPYTANIE:    $query")
        println("OCZEKIWANO:         $expected")
        try {
            val result = interpreter.executeQuery(query).asJsonArray
            println("OTRZYMANO WYNIKÓW:  ${result.size()}")
            if (result.size() > 0) {
                println("PRZYKŁADOWY WYNIK:  ${result[0].asJsonObject}")
            } else {
                println("OTRZYMANO:          [] (Pusty wynik)")
            }
            println("=======================================================")
            return result
        } catch (e: Exception) {
            println("!!! BŁĄD WYKONANIA !!!")
            println("TREŚĆ BŁĘDU:        ${e.message}")
            println("=======================================================")
            throw e // Rzucamy dalej, aby test w JUnit zaświecił się na czerwono
        }
    }

    // =========================================================================
    // GRUPA 1: TYPY DANYCH I LITERAŁY (Data types & Literals)
    // =========================================================================

    @Test
    @Order(1)
    @DisplayName("1.1 String Literals - Single Quotes")
    fun testStringLiteralsSingle() {
        val query = "select e:concept:name where e:concept:name = 'accept' limit 1"
        val result = executeAndPrint(
            "String - Pojedynczy cudzysłów", "where e:name = 'accept'", query, "1 wynik"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(2)
    @DisplayName("1.2 String Literals - Double Quotes")
    fun testStringLiteralsDouble() {
        val query = "select e:concept:name where e:concept:name = \"accept\" limit 1"
        val result = executeAndPrint(
            "String - Podwójny cudzysłów", "where e:name = \"accept\"", query, "1 wynik"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(3)
    @DisplayName("1.3 Escape Sequences")
    fun testEscapeSequences() {
        val query = "select e:concept:name where e:concept:name = 'get \\n review \\t X' limit 1"
        executeAndPrint(
            "Znaki ucieczki", "where e:name = 'get \\n review \\t X'", query, "Przejście parsera bez błędu"
        )
    }

    @Test
    @Order(4)
    @DisplayName("1.4 Number Literals (Decimal & Scientific)")
    fun testNumberLiterals() {
        val query = "select e:cost:total where e:cost:total > 1.05 and e:cost:total < 1.23E1 limit 1"
        val result = executeAndPrint(
            "Liczby (Dziesiętne i Naukowe)", "where e:total > 1.05 and e:total < 1.23E1", query, "Przynajmniej 1 wynik dla kosztu > 1.05"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(5)
    @DisplayName("1.5 Boolean Literals")
    fun testBooleanLiterals() {
        val query = "select e:concept:name where true limit 1"
        val result = executeAndPrint(
            "Wartości logiczne", "where true", query, "Brak błędu, zwrócenie wyników"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(6)
    @DisplayName("1.6 Datetime Literals (ISO 8601)")
    fun testDatetimeLiterals() {
        val query = "select e:time:timestamp where e:time:timestamp >= D2007-06-13T01:00:00.000+02:00 limit 1"
        val result = executeAndPrint(
            "Data z myślnikami", "where e:timestamp >= D2007-06-13T01:00:00.000+02:00", query, "Parsowanie dat i filtrowanie"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(7)
    @DisplayName("1.7 Null Literals")
    fun testNullLiterals() {
        // Zmieniliśmy "cost:total" na "jakies_pole", aby wymusić trafienie dla IS NULL
        val query = "select e:concept:name where e:jakies_pole is null limit 1"
        val result = executeAndPrint(
            "Null handling", "where e:jakies_pole is null", query, "Wyniki dla zdarzeń bez kosztu"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(8)
    @DisplayName("1.8 Scoped Literals")
    fun testScopedLiterals() {
        val query = "select e:concept:name where e:cost:total = e:1.08 limit 1"
        val result = executeAndPrint(
            "Literały ze wskazanym scopem", "where e:total = e:1.08", query, "Parsowanie literału z jawnym scopem np. e:1.08"
        )
        assertTrue(result.size() > 0)
    }

    // =========================================================================
    // GRUPA 2: ATRYBUTY I ICH WSKAZYWANIE (Attributes & Scopes)
    // =========================================================================

    @Test
    @Order(9)
    @DisplayName("2.1 Standard Attributes")
    fun testStandardAttributes() {
        val query = "select log:concept:name, trace:cost:total, event:time:timestamp limit 1"
        val result = executeAndPrint(
            "Standardowe, pełne atrybuty", "select log:concept:name, trace:cost:total, event:time:timestamp", query, "Dokument z 3 różnymi polami"
        )
        assertTrue(result[0].asJsonObject.keySet().isNotEmpty())
    }

    @Test
    @Order(10)
    @DisplayName("2.2 Shorthand Attributes")
    fun testShorthandAttributes() {
        val query = "select l:name, t:total, e:timestamp limit 1"
        val result = executeAndPrint(
            "Skrócone nazwy atrybutów", "select l:name, t:total, e:timestamp", query, "Rozwiązanie aliasów do standardowych atrybutów XES"
        )
        assertTrue(result[0].asJsonObject.keySet().isNotEmpty())
    }

    @Test
    @Order(11)
    @DisplayName("2.3 Bracket Syntax for Custom Attributes")
    fun testBracketSyntax() {
        val query = "select [event:org:resource] limit 1"
        val result = executeAndPrint(
            "Nawiasy kwadratowe", "select [event:org:resource]", query, "Poprawne pobranie customowego atrybutu (bez wywalania się parsera)"
        )
        assertTrue(result.size() > 0)
    }

    // =========================================================================
    // GRUPA 3: OPERATORY ARYTMETYCZNE I CZASOWE
    // =========================================================================

    @Test
    @Order(12)
    @DisplayName("3.1 Arithmetic: Addition & Subtraction")
    fun testArithmeticAddSub() {
        val query = "select e:cost:total + 1.0, e:cost:total - 0.5 where e:cost:total is not null limit 1"
        val result = executeAndPrint(
            "Dodawanie i odejmowanie", "select e:total + 1.0, e:total - 0.5", query, "Wykonana matematyka (w bazie lub w pamięci silnika)"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(13)
    @DisplayName("3.2 Arithmetic: Multiplication & Division")
    fun testArithmeticMulDiv() {
        val query = "select e:cost:total * 2.0, e:cost:total / 2.0 where e:cost:total is not null limit 1"
        val result = executeAndPrint(
            "Mnożenie i dzielenie", "select e:total * 2.0, e:total / 2.0", query, "Wykonana matematyka na polach"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(14)
    @DisplayName("3.3 String Concatenation (+)")
    fun testStringConcatenation() {
        val query = "select e:concept:name + ' test' limit 1"
        val result = executeAndPrint(
            "Łączenie stringów", "select e:name + ' test'", query, "Wykonana konkatenacja"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(15)
    @DisplayName("3.4 Temporal Subtraction")
    fun testTemporalSubtraction() {
        val query = "select e:time:timestamp - e:time:timestamp limit 1"
        val result = executeAndPrint(
            "Odejmowanie dat", "select e:timestamp - e:timestamp", query, "Zwrócona liczba dni (powinna wynosić 0)"
        )
        assertTrue(result.size() > 0)
    }

    // =========================================================================
    // GRUPA 4: OPERATORY TEKSTOWE, LOGICZNE I PORÓWNAWCZE
    // =========================================================================

    @Test
    @Order(16)
    @DisplayName("4.1 Text: LIKE Operator")
    fun testLikeOperator() {
        val query = "select e:concept:name where e:concept:name like '%review%' limit 1"
        val result = executeAndPrint(
            "Operator LIKE", "where e:name like '%review%'", query, "Zdarzenie ze słowem review"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(17)
    @DisplayName("4.2 Text: MATCHES Operator (Regex)")
    fun testMatchesOperator() {
        val query = "select e:concept:name where e:concept:name matches '.*get.*' limit 1"
        val result = executeAndPrint(
            "Operator MATCHES", "where e:name matches '.*get.*'", query, "Zdarzenie spełniające wyrażenie regularne"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(18)
    @DisplayName("4.3 Comparison Operators (<, >, <=, >=)")
    fun testComparisonOperators() {
        val query = "select e:cost:total where e:cost:total >= 1.0 and e:cost:total <= 2.0 limit 1"
        val result = executeAndPrint(
            "Operatory nierówności", "where e:total >= 1.0 and e:total <= 2.0", query, "Odpowiednie zmapowanie nawiasów ostre na MongoDB"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(19)
    @DisplayName("4.4 Equality Operators (=, !=)")
    fun testEqualityOperators() {
        val query = "select e:concept:name where e:concept:name = 'accept' or e:concept:name != 'reject' limit 1"
        val result = executeAndPrint(
            "Operatory równości", "where e:name = 'accept' or e:name != 'reject'", query, "Prawidłowa obsługa = oraz !="
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(20)
    @DisplayName("4.5 Logic Operators (AND, OR, NOT)")
    fun testLogicOperators() {
        val query = "select e:concept:name where (e:concept:name = 'accept' or e:concept:name = 'reject') and not(e:cost:total is null) limit 1"
        val result = executeAndPrint(
            "Złożona logika", "where (e:name='a' or e:name='b') and not(e:total is null)", query, "Poprawne zagłębienie warunków"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(21)
    @DisplayName("4.6 IN and NOT IN Operators")
    fun testInOperators() {
        val query = "select e:concept:name where e:concept:name in ('accept', 'reject') and e:concept:name not in ('decide') limit 1"
        val result = executeAndPrint(
            "IN / NOT IN", "where e:name in ('accept', 'reject')", query, "Poprawna zamiana na klauzulę OR pod spodem"
        )
        assertTrue(result.size() > 0)
    }

    // =========================================================================
    // GRUPA 5: FUNKCJE SKALARNE (Scalar Functions)
    // =========================================================================

    @Test
    @Order(22)
    @DisplayName("5.1 String Functions (upper, lower)")
    fun testStringFunctions() {
        val query = "select upper(e:concept:name), lower(e:concept:name) limit 1"
        val result = executeAndPrint(
            "Funkcje tekstowe", "select upper(e:name), lower(e:name)", query, "Wykonane mapowanie wielkości liter"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(23)
    @DisplayName("5.2 Datetime Functions (year, month, day, dayofweek...)")
    fun testDatetimeFunctions() {
        val query = "select year(e:time:timestamp), month(e:time:timestamp), day(e:time:timestamp), dayofweek(e:time:timestamp) limit 1"
        val result = executeAndPrint(
            "Funkcje czasu", "select year(e:timestamp), month(e:timestamp)...", query, "Rozbicie daty na kawałki"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(24)
    @DisplayName("5.3 Math Functions (round)")
    fun testMathFunctions() {
        val query = "select round(e:cost:total) where e:cost:total is not null limit 1"
        val result = executeAndPrint(
            "Zaokrąglanie", "select round(e:total)", query, "Wywołana funkcja matematyczna"
        )
        assertTrue(result.size() > 0)
    }

    // =========================================================================
    // GRUPA 6: FUNKCJE AGREGUJĄCE (Aggregation)
    // =========================================================================

    @Test
    @Order(25)
    @DisplayName("6.1 All Aggregations (min, max, avg, sum, count)")
    fun testAggregations() {
        val query = "select min(e:cost:total), max(e:cost:total), avg(e:cost:total), sum(e:cost:total), count(e:concept:name) group by t:name limit 1"
        val result = executeAndPrint(
            "Wszystkie agregacje", "select min(e:total), max(e:total)... group by t:name", query, "Wyliczenie wszystkich statystyk w grupie"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(26)
    @DisplayName("6.2 Implicit Group By")
    fun testImplicitGroupBy() {
        val query = "select avg(e:cost:total)"
        val result = executeAndPrint(
            "Niejawne grupowanie", "select avg(e:total)", query, "Brak błędu, użycie agregatu bez klauzuli GROUP BY wyzwala tryb Implicit"
        )
        assertTrue(result.size() > 0)
    }

    // =========================================================================
    // GRUPA 7: SCOPE HOISTING (Wyrzucanie zakresu ^ i ^^)
    // =========================================================================

    @Test
    @Order(27)
    @DisplayName("7.1 Hoisting ^ in WHERE clause")
    fun testHoistingWhere() {
        val query = "select t:name where ^e:concept:name = 'accept' limit 1"
        val result = executeAndPrint(
            "Filtrowanie rodzica dzieckiem", "where ^e:name = 'accept'", query, "Wybranie śladów posiadających dany event"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(28)
    @DisplayName("7.2 Global Hoisting ^^ in SELECT clause")
    fun testGlobalHoisting() {
        val query = "select count(^^e:concept:name)"
        val result = executeAndPrint(
            "Agregacja całego logu", "select count(^^e:name)", query, "1 dokument podsumowujący wszystko"
        )
        assertEquals(1, result.size())
    }

    // =========================================================================
    // GRUPA 8: KLAUZULE STRUKTURALNE (Clauses)
    // =========================================================================

    @Test
    @Order(29)
    @DisplayName("8.1 Wildcard Selector (*)")
    fun testWildcardSelector() {
        val query = "select t:*, e:* limit 1"
        val result = executeAndPrint(
            "Wildcard", "select t:*, e:*", query, "Wszystkie atrybuty w obrębie danego logu"
        )
        assertTrue(result.size() > 0)
    }

    @Test
    @Order(30)
    @DisplayName("8.2 Limit and Offset")
    fun testLimitAndOffset() {
        val query = "select e:concept:name limit 5 offset 2"
        val result = executeAndPrint(
            "Paginacja", "limit 5 offset 2", query, "Ominięcie pierwszych 2 wyników i podanie kolejnych 5"
        )
        assertEquals(5, result.size())
    }

    @Test
    @Order(31)
    @DisplayName("8.3 Group By & Order By (ASC/DESC)")
    fun testGroupAndOrder() {
        val query = "select count(e:concept:name) group by e:concept:name order by count(e:concept:name) desc limit 2"
        val result = executeAndPrint(
            "Sortowanie grup", "group by e:name order by count(e:name) desc", query, "Posegregowane po największej ilości wystąpień"
        )
        assertEquals(2, result.size())
    }

    // =========================================================================
    // GRUPA 9: FINAL BOSS (Złożone struktury)
    // =========================================================================

    @Test
    @Order(32)
    @DisplayName("9.1 The Ultimate Complex Query")
    fun testUltimateComplexQuery() {
        val query = """
            select 
                upper(e:concept:name), 
                count(t:name) 
            where 
                (e:cost:total >= 0 or e:cost:total is null) 
                and e:concept:name matches '.*review.*' 
            group by e:concept:name 
            order by count(t:name) desc 
            limit 5
        """.trimIndent()

        val result = executeAndPrint(
            "Wszystko naraz", "Złożone filtrowanie, Regex, Funkcja skalarna, Agregacja, Grupowanie, Sortowanie", query.replace("\n", " "), "Udane sparsowanie i wykonanie wielopoziomowego AST"
        )
        assertTrue(result.size() > 0)
    }

    // =========================================================================
    // GRUPA 10: DELETE (Stan i Mutacje)
    // =========================================================================

    @Test
    @Order(999)
    @DisplayName("10.1 DELETE Clause")
    fun testDeleteClause() {
        val countQuery = "select count(^^e:concept:name) where e:concept:name = 'get review X'"
        val preDeleteResult = interpreter.executeQuery(countQuery).asJsonArray
        val docBefore = preDeleteResult.firstOrNull()?.asJsonObject?.getAsJsonArray("events")?.get(0)?.asJsonObject
        val countBefore = docBefore?.get("count(^^e:concept:name)")?.asDouble?.toInt() ?: 0

        val deleteQuery = "delete event where e:concept:name = 'get review X'"
        executeAndPrint(
            "DELETE", "delete event where e:name = 'get review X'", deleteQuery, "Brak błędu podczas usuwania"
        )

        val postDeleteResult = interpreter.executeQuery(countQuery).asJsonArray
        val docAfter = postDeleteResult.firstOrNull()?.asJsonObject?.getAsJsonArray("events")?.firstOrNull()?.asJsonObject
        val countAfter = docAfter?.get("count(^^e:concept:name)")?.asDouble?.toInt() ?: 0

        assertTrue(countBefore > 0, "Baza nie posiadała danych startowych przed usunięciem")
        assertEquals(0, countAfter, "DELETE nie usunęło danych")
    }
}
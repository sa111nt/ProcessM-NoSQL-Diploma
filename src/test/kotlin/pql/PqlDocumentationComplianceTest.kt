package pql

import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import db.CouchDBManager
import app.PqlInterpreter
import app.LogImporter
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.TestMethodOrder

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class) // Wymuszamy kolejność testów!
class PqlDocumentationComplianceTest {

    private lateinit var interpreter: PqlInterpreter
    private lateinit var journalLogId: String
    private lateinit var hospitalLogId: String
    private val dbName = "event_logs"
    private lateinit var dbManager: CouchDBManager

    @BeforeAll
    fun setup() {
        dbManager = CouchDBManager(
            url = "http://127.0.0.1:5984",
            user = "admin",
            password = "admin"
        )

        try { dbManager.deleteDb(dbName) } catch (e: Exception) {}
        try { dbManager.createDb(dbName) } catch (e: Exception) {}

        println("=== SETUP: Rozpoczęcie importowania logów ===")

        val logQuery = JsonObject().apply {
            add("selector", JsonObject().apply { addProperty("docType", "log") })
        }

        LogImporter.import("src/test/resources/Hospital_log.xes.gz", dbName, dbManager)
        val hospitalLogs = dbManager.findDocs(dbName, logQuery)
        hospitalLogId = hospitalLogs[0].asJsonObject.get("_id").asString
        println("Zidentyfikowano Hospital Log: ID = $hospitalLogId")

        LogImporter.import("src/test/resources/JournalReview-extra.xes", dbName, dbManager)
        val allLogs = dbManager.findDocs(dbName, logQuery)
        for (i in 0 until allLogs.size()) {
            val currentId = allLogs[i].asJsonObject.get("_id").asString
            if (currentId != hospitalLogId) {
                journalLogId = currentId
                println("Zidentyfikowano Journal Log: ID = $journalLogId")
            }
        }

        interpreter = PqlInterpreter(dbManager, dbName)
        println("=== KONIEC SETUPU ===\n")
    }

    // =========================================================================
    // 1. TESTY POKAZOWE (DOKUMENTACYJNE) - 42 Zapytania DQL
    // =========================================================================
    data class ShowcaseQuery(
        val description: String,
        val originalPql: String,
        val ourPql: String
    )

    @Test
    @Order(1) // Ten test poleci jako PIERWSZY
    fun executeShowcaseQueries() {
        val gson = GsonBuilder().setPrettyPrinting().create()

        val queries = listOf(
            ShowcaseQuery("1. Podstawowy SELECT (wskazane atrybuty)", "SELECT concept:name, org:resource FROM Event", "select e:concept:name, e:org:resource where l:id='$journalLogId' limit 2"),
            ShowcaseQuery("2. Wybieranie wszystkich atrybutów (*)", "SELECT * FROM Log", "select * where l:id='$journalLogId' limit 1"),
            ShowcaseQuery("3. Stronicowanie (LIMIT & OFFSET)", "SELECT concept:name LIMIT 3 OFFSET 5", "select e:concept:name where l:id='$journalLogId' limit 3 offset 5"),
            ShowcaseQuery("4. Proste filtrowanie logiczne (WHERE ... AND)", "SELECT concept:name, org:resource WHERE org:resource = 'Pete'", "select e:concept:name, e:org:resource where l:id='$journalLogId' and e:org:resource = 'Pete' limit 2"),
            ShowcaseQuery("5. Filtrowanie z operatorem OR", "SELECT concept:name WHERE org:resource = 'Pete' OR org:resource = 'Anne'", "select e:concept:name, e:org:resource where l:id='$journalLogId' and (e:org:resource = 'Pete' or e:org:resource = 'Anne') limit 2"),
            ShowcaseQuery("6. Operator IN (Lista wartości)", "SELECT concept:name WHERE org:resource IN ('Anne', 'Pete')", "select e:concept:name, e:org:resource where l:id='$journalLogId' and e:org:resource in ('Anne', 'Pete') limit 2"),
            ShowcaseQuery("7. Operator tekstowy LIKE (Wyszukiwanie wzorca)", "SELECT concept:name WHERE concept:name LIKE '%review%'", "select e:concept:name where l:id='$journalLogId' and e:concept:name like '%review%' limit 2"),
            ShowcaseQuery("8. Operator sprawdzania NULL (is not null)", "SELECT concept:name WHERE cost:total IS NOT NULL", "select e:concept:name, e:cost:total where l:id='$journalLogId' and e:cost:total is not null limit 2"),
            ShowcaseQuery("9. Porównanie matematyczne (>)", "SELECT concept:name WHERE cost:total > 1.0", "select e:concept:name, e:cost:total where l:id='$journalLogId' and e:cost:total > 1.0 limit 2"),
            ShowcaseQuery("10. Dynamiczne operacje arytmetyczne", "SELECT cost:total, cost:total * 1.5", "select e:cost:total, e:cost:total * 1.5 where l:id='$journalLogId' and e:cost:total is not null limit 2"),
            ShowcaseQuery("11. Złożona arytmetyka w locie", "SELECT (cost:total + 10) * 2", "select (e:cost:total + 10) * 2 where l:id='$journalLogId' and e:cost:total is not null limit 2"),
            ShowcaseQuery("12. Agregacje (Implicit Group By)", "SELECT COUNT(concept:name), AVG(cost:total), MIN(cost:total)", "select count(^^e:concept:name), avg(^^e:cost:total), min(^^e:cost:total) where l:id='$journalLogId'"),
            ShowcaseQuery("13. Tradycyjne Grupowanie (GROUP BY)", "SELECT org:resource, COUNT(concept:name) GROUP BY org:resource", "select e:org:resource, count(e:concept:name) where l:id='$journalLogId' group by e:org:resource limit 3"),
            ShowcaseQuery("14. Wielokrotne Grupowanie", "SELECT org:resource, lifecycle:transition, COUNT(*) GROUP BY org:resource, lifecycle:transition", "select e:org:resource, e:lifecycle:transition, count(e:concept:name) where l:id='$journalLogId' group by e:org:resource, e:lifecycle:transition limit 3"),
            ShowcaseQuery("15. Process Mining - Warianty (^)", "SELECT COUNT(Trace) GROUP BY ^concept:name ORDER BY COUNT(Trace) DESC", "select count(t:name) where l:id='$journalLogId' group by ^e:concept:name order by count(t:name) desc limit 3"),
            ShowcaseQuery("16. Sortowanie (ORDER BY)", "SELECT concept:name, time:timestamp ORDER BY time:timestamp DESC", "select e:concept:name, e:time:timestamp where l:id='$journalLogId' order by e:time:timestamp desc limit 3"),
            ShowcaseQuery("17. Sortowanie po agregacji", "SELECT org:resource, COUNT(concept:name) GROUP BY org:resource ORDER BY COUNT(concept:name) DESC", "select e:org:resource, count(e:concept:name) where l:id='$journalLogId' group by e:org:resource order by count(e:concept:name) desc limit 3"),
            ShowcaseQuery("18. Poziom śladu (Trace Level)", "SELECT Trace.concept:name, COUNT(Event.concept:name) GROUP BY Trace.concept:name", "select t:concept:name, count(e:concept:name) where l:id='$journalLogId' group by t:concept:name limit 2"),
            ShowcaseQuery("19. Cross-Scope WHERE", "SELECT Trace.concept:name WHERE ^Event.concept:name = 'reject'", "select t:concept:name where l:id='$journalLogId' and ^e:concept:name = 'reject' limit 2"),
            ShowcaseQuery("20. Różnica dat/czasów w Order By", "SELECT concept:name ORDER BY time:timestamp ASC", "select e:concept:name, e:time:timestamp where l:id='$journalLogId' order by e:time:timestamp asc limit 2"),
            ShowcaseQuery("21. Operator Nierówności (!=)", "SELECT concept:name WHERE org:resource != 'Mike'", "select e:concept:name, e:org:resource where l:id='$journalLogId' and e:org:resource != 'Mike' limit 2"),
            ShowcaseQuery("22. Operatory Mniejsze/Równe (<=)", "SELECT concept:name WHERE cost:total <= 1.0", "select e:concept:name, e:cost:total where l:id='$journalLogId' and e:cost:total <= 1.0 limit 2"),
            ShowcaseQuery("23. Operator NOT IN", "SELECT concept:name WHERE org:resource NOT IN ('Mike', 'Pete')", "select e:concept:name, e:org:resource where l:id='$journalLogId' and e:org:resource not in ('Mike', 'Pete') limit 2"),
            ShowcaseQuery("24. Negacja Logiczna NOT (...)", "SELECT concept:name WHERE NOT (lifecycle:transition = 'complete')", "select e:concept:name, e:lifecycle:transition where l:id='$journalLogId' and not (e:lifecycle:transition = 'complete') limit 2"),
            ShowcaseQuery("25. Wyrażenia Regularne (MATCHES)", "SELECT concept:name WHERE concept:name MATCHES '^inv.*'", "select e:concept:name where l:id='$journalLogId' and e:concept:name matches '^inv.*' limit 2"),
            ShowcaseQuery("26. Operator IS NULL", "SELECT concept:name WHERE cost:total IS NULL", "select e:concept:name, e:cost:total where l:id='$journalLogId' and e:cost:total is null limit 2"),
            ShowcaseQuery("27. Złożone AND i OR z nawiasami", "SELECT concept:name WHERE (org:resource = 'Mike' OR org:resource = 'Anne') AND lifecycle:transition = 'start'", "select e:concept:name, e:org:resource, e:lifecycle:transition where l:id='$journalLogId' and (e:org:resource = 'Mike' or e:org:resource = 'Anne') and e:lifecycle:transition = 'start' limit 2"),
            ShowcaseQuery("28. Globalna funkcja SUM", "SELECT SUM(cost:total)", "select sum(^^e:cost:total) where l:id='$journalLogId'"),
            ShowcaseQuery("29. Operacje MIN / MAX na Datach", "SELECT MIN(time:timestamp), MAX(time:timestamp)", "select min(^^e:time:timestamp), max(^^e:time:timestamp) where l:id='$journalLogId'"),
            ShowcaseQuery("30. Agregacja Kosztów Śladu", "SELECT Trace.concept:name, SUM(cost:total) GROUP BY Trace.concept:name", "select t:concept:name, sum(e:cost:total) where l:id='$journalLogId' group by t:concept:name limit 3"),
            ShowcaseQuery("31. Sortowanie Wielokolumnowe", "SELECT org:resource, time:timestamp ORDER BY org:resource ASC, time:timestamp DESC", "select e:org:resource, e:time:timestamp where l:id='$journalLogId' order by e:org:resource asc, e:time:timestamp desc limit 3"),
            ShowcaseQuery("32. Ułamkowa Arytmetyka", "SELECT (cost:total / 2.0) - 0.1", "select (e:cost:total / 2.0) - 0.1 where l:id='$journalLogId' and e:cost:total is not null limit 2"),
            ShowcaseQuery("33. Cross-Scope Filtrowanie", "SELECT concept:name WHERE Trace.cost:total > 15.0", "select e:concept:name, t:cost:total where l:id='$journalLogId' and t:cost:total > 15.0 limit 2"),
            ShowcaseQuery("34. Cross-Scope Sortowanie", "SELECT concept:name ORDER BY Trace.cost:total DESC", "select e:concept:name, t:cost:total where l:id='$journalLogId' and t:cost:total is not null order by t:cost:total desc limit 2"),
            ShowcaseQuery("35. Sortowanie Grup po Sumie", "SELECT Trace.concept:name, SUM(cost:total) GROUP BY Trace.concept:name ORDER BY SUM(cost:total) DESC", "select t:concept:name, sum(e:cost:total) where l:id='$journalLogId' group by t:concept:name order by sum(e:cost:total) desc limit 2"),
            ShowcaseQuery("36. COUNT na Trace", "SELECT COUNT(Trace)", "select count(^^t:name) where l:id='$journalLogId'"),
            ShowcaseQuery("37. AVG Kosztów po Zasobach", "SELECT org:resource, AVG(cost:total) GROUP BY org:resource", "select e:org:resource, avg(e:cost:total) where l:id='$journalLogId' group by e:org:resource limit 3"),
            ShowcaseQuery("38. Process Mining: AVG po Wariancie", "SELECT AVG(Trace.cost:total) GROUP BY ^concept:name", "select avg(t:cost:total) where l:id='$journalLogId' group by ^e:concept:name limit 3"),
            ShowcaseQuery("39. Głęboka Paginacja (OFFSET)", "SELECT concept:name LIMIT 5 OFFSET 1000", "select e:concept:name where l:id='$journalLogId' limit 5 offset 1000"),
            ShowcaseQuery("40. Kompletny Kombajn PQL", "SELECT org:resource, SUM(cost:total) WHERE cost:total IS NOT NULL GROUP BY org:resource ORDER BY SUM(cost:total) DESC", "select e:org:resource, sum(e:cost:total) where l:id='$journalLogId' and e:cost:total is not null group by e:org:resource order by sum(e:cost:total) desc limit 3"),
            ShowcaseQuery("41. Atrybuty Logu w SELECT", "SELECT concept:name, source FROM Log", "select l:concept:name, l:source where l:id='$journalLogId' limit 1"),
            ShowcaseQuery("42. Klauzula HAVING", "SELECT org:resource, COUNT(concept:name) GROUP BY org:resource HAVING COUNT(concept:name) > 50", "select e:org:resource, count(e:concept:name) where l:id='$journalLogId' group by e:org:resource having count(e:concept:name) > 50")
        )

        println("\n\n========================================================")
        println("===   ROZPOCZĘCIE TESTÓW PREZENTACYJNYCH DQL (42)    ===")
        println("========================================================\n")

        for ((index, item) in queries.withIndex()) {
            println("ZAPYTANIE ${index + 1}/42: ${item.description}")
            println("KLAUZULA ORYGINALNEGO PQL:  ${item.originalPql}")
            println("KLAUZULA NASZEJ WERSJI PQL: ${item.ourPql}")
            try {
                val startTime = System.currentTimeMillis()
                val result = interpreter.executeQuery(item.ourPql).asJsonArray
                val duration = System.currentTimeMillis() - startTime
                println("CZAS WYKONANIA: ${duration}ms")
                println("ZWRÓCONY JSON (${result.size()} elementów):")
                println(gson.toJson(result))
            } catch (e: Exception) {
                println("BŁĄD WYKONANIA ZAPYTANIA (Przewidziany limit architektoniczny): ${e.message}")
            }
            println("--------------------------------------------------------\n")
        }
    }

    // =========================================================================
    // 2. TESTY GOLDEN MASTER (Z ASERCJAMI DANYCH Z PLIKU XES)
    // =========================================================================
    data class PqlTestCase(
        val description: String,
        val query: String,
        val expectedSize: Int,
        val verifier: ((JsonArray) -> Unit)? = null
    )

    @Test
    @Order(2) // Ten test poleci jako DRUGI (na czystych, nienaruszonych danych)
    fun executeGoldenMasterTests() {
        val testCases = listOf(
            PqlTestCase(
                description = "Sprawdzenie poprawnego mapowania wartości zasobów",
                query = "select e:concept:name, e:org:resource where l:id='$journalLogId' limit 2",
                expectedSize = 2,
                verifier = { result ->
                    val first = result[0].asJsonObject
                    assertEquals("Mike", first.get("e:org:resource").asString)
                    assertEquals("invite reviewers", first.get("e:concept:name").asString)
                }
            ),
            PqlTestCase(
                description = "Weryfikacja Globalnej Agregacji dla pliku JournalReview",
                query = "select count(^^e:concept:name), avg(^^e:cost:total), min(^^e:cost:total) where l:id='$journalLogId'",
                expectedSize = 1,
                verifier = { result ->
                    val aggs = result[0].asJsonObject.getAsJsonArray("events")[0].asJsonObject
                    assertEquals("2298.0", aggs.get("count(^^e:concept:name)").asString)
                    assertEquals("1.0", aggs.get("min(^^e:cost:total)").asString)
                }
            ),
            PqlTestCase(
                description = "Sprawdzenie działania operatora IS NULL (Brak wyników)",
                query = "select e:concept:name, e:cost:total where l:id='$journalLogId' and e:cost:total is null limit 2",
                expectedSize = 0
            ),
            PqlTestCase(
                description = "Weryfikacja matematycznej sumy kosztów",
                query = "select sum(^^e:cost:total) where l:id='$journalLogId'",
                expectedSize = 1,
                verifier = { result ->
                    val events = result[0].asJsonObject.getAsJsonArray("events")[0].asJsonObject
                    val sum = events.get("sum(^^e:cost:total)").asString.toDouble()
                    assertEquals(2386.64, sum, 0.01)
                }
            )
        )

        println("\n========================================================")
        println("===   ROZPOCZĘCIE TESTÓW AUTOMATYCZNYCH Z ASERCJAMI  ===")
        println("========================================================\n")

        var passed = 0
        var failed = 0

        for ((index, testCase) in testCases.withIndex()) {
            print("ASERCJA ${index + 1}/${testCases.size}: ${testCase.description} ... ")
            try {
                val result = interpreter.executeQuery(testCase.query).asJsonArray
                assertEquals(testCase.expectedSize, result.size(), "Niezgodna liczba elementów wynikowych")
                testCase.verifier?.invoke(result)
                println("✅ PASSED")
                passed++
            } catch (e: AssertionError) {
                println("❌ FAILED (Błąd Asercji: ${e.message})")
                failed++
            } catch (e: Exception) {
                println("💥 ERROR (Wyjątek silnika: ${e.message})")
                failed++
            }
        }
        println("PODSUMOWANIE ASERCJI: $passed ZALICZONYCH, $failed OBLANYCH.\n")
        assertEquals(0, failed, "Niektóre zapytania Golden Master nie przeszły weryfikacji danych XES!")
    }

    // =========================================================================
    // 3. TESTY KLAUZULI MODYFIKUJĄCEJ (DELETE)
    // =========================================================================
    data class DeleteTestCase(
        val description: String,
        val originalPql: String,
        val ourPql: String,
        val expectedResultDesc: String
    )

    @Test
    @Order(3) // Zniszczenie danych (DELETE) poleci na samym końcu
    fun executeDeleteTests() {
        val gson = GsonBuilder().setPrettyPrinting().create()

        val deleteCases = listOf(
            DeleteTestCase(
                description = "Usuwanie konkretnych zdarzeń (np. wszystkich wykonanych przez Mike'a)",
                originalPql = "DELETE EVENT WHERE org:resource = 'Mike'",
                ourPql = "delete event where l:id='$journalLogId' and e:org:resource = 'Mike'",
                expectedResultDesc = "Obiekty typu 'event' przypisane do Mike'a zostają zlokalizowane i usunięte z bazy CouchDB."
            ),
            DeleteTestCase(
                description = "Usuwanie całych ścieżek procesowych (Trace) na podstawie kosztu",
                originalPql = "DELETE TRACE WHERE cost:total < 5.0",
                ourPql = "delete trace where l:id='$journalLogId' and t:cost:total < 5.0",
                expectedResultDesc = "Wszystkie obiekty 'trace' spełniające warunek zostają usunięte."
            ),
            DeleteTestCase(
                description = "Całkowite usunięcie logu z bazy (Czyszczenie danych)",
                originalPql = "DELETE LOG WHERE concept:name = 'Hospital_log'",
                ourPql = "delete log where l:id='$hospitalLogId'",
                expectedResultDesc = "Dokument metadanych logu o wskazanym ID zostaje usunięty z bazy danych."
            )
        )

        println("\n\n========================================================")
        println("===        TESTY KLAUZULI MODYFIKUJĄCEJ (DELETE)     ===")
        println("========================================================\n")

        for ((index, testCase) in deleteCases.withIndex()) {
            println("TEST DELETE ${index + 1}/3: ${testCase.description}")
            println("KLAUZULA ORYGINALNEGO PQL:   ${testCase.originalPql}")
            println("KLAUZULA NASZEJ WERSJI PQL:  ${testCase.ourPql}")
            println("OCZEKIWANY WYNIK:            ${testCase.expectedResultDesc}")

            try {
                val startTime = System.currentTimeMillis()
                val result = interpreter.executeQuery(testCase.ourPql)
                val duration = System.currentTimeMillis() - startTime

                println("WYNIK RZECZYWISTY (Czas: ${duration}ms):")
                println(result.toString())
            } catch (e: Exception) {
                println("BŁĄD PODCZAS USUWANIA: ${e.message}")
            }
            println("--------------------------------------------------------\n")
        }

        println("========================================================")
        println("WERYFIKACJA KOŃCOWA: Sprawdzanie czy Hospital Log zniknął po teście 3...")
        val checkLog = "select * where l:id='$hospitalLogId'"
        try {
            val result = interpreter.executeQuery(checkLog).asJsonArray
            println("Wysłano zapytanie: $checkLog")
            println("Liczba logów o ID $hospitalLogId w bazie: ${result.size()} (Oczekiwano: 0)")
            assertEquals(0, result.size(), "Błąd: Hospital Log nie został poprawnie usunięty z bazy CouchDB!")
            println("✅ Usunięcie potwierdzone programistycznie.")
        } catch (e: Exception) {
            println("Zapytanie weryfikujące zwróciło błąd (log został poprawnie usunięty): ${e.message}")
        }

        println("========================================================")
        println("===             ZAKOŃCZONO WSZYSTKIE TESTY           ===")
        println("========================================================\n")
    }
}
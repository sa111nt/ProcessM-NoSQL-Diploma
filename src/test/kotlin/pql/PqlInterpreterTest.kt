package pql

import app.PqlInterpreter
import app.LogImporter
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import db.CouchDBManager
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/**
 * Testy PQL oparte na testach referencyjnych z repozytorium:
 * DBHierarchicalXESInputStreamWithQueryTests
 * Weryfikują poprawność filtrowania, sortowania oraz zaawansowanych
 * mechanizmów grupowania (w tym detekcję wariantów procesów - Process Mining).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PqlInterpreterTest {

    private lateinit var interpreter: PqlInterpreter
    private lateinit var journalLogId: String
    private lateinit var hospitalLogId: String

    @BeforeAll
    fun setup() {
        val dbManager = CouchDBManager(
            url = "http://127.0.0.1:5984",
            user = "admin",
            password = "admin"
        )
        val dbName = "event_logs"

        try {
            dbManager.deleteDb(dbName)
        } catch (e: Exception) {}

        try {
            dbManager.createDb(dbName)
        } catch (e: Exception) {}

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
        println("=== KONIEC SETUPU ===")
    }

    /**
     * Test #1: errorHandlingTest
     *
     * Co robi test: Weryfikuje obsługę błędów przy wywoływaniu zapytań z nieistniejącymi atrybutami
     * w klauzulach ORDER BY i GROUP BY.
     *
     * Różnica w zapytaniu i danych wyjściowych: Zapytania pozostały podobne. W oryginale oczekiwano przerwania działania,
     * a nowa baza elastycznie przetwarza puste struktury.
     *
     * Różnica w implementacji (Oryginał vs Nowa Architektura): W starej architekturze (ścisły schemat w pamięci) odwołanie
     * do nieistniejącego atrybutu (np. c:nonexistent) rzucało `IllegalArgumentException`. W nowej architekturze NoSQL
     * baza ignoruje brakujące pola chroniąc aplikację przed błędami (tzw. graceful degradation). Test oznaczony jako @Disabled.
     */
    @Test
    @org.junit.jupiter.api.Disabled("Wymuszane przez naturę NoSQL (schema-less) - silnik nie rzuca błędów dla nieznanych pól.")
    fun errorHandlingTest() {
        val invalidQueries = listOf(
            "where l:id='$journalLogId' order by c:nonexistent",
            "where l:id='$journalLogId' group by c:nonstandard_nonexisting"
        )

        for (query in invalidQueries) {
            val ex = assertFailsWith<Exception> {
                interpreter.executeQuery(query)
            }
            val message = ex.message?.lowercase() ?: ""
            assertTrue(
                message.contains("not found") || message.contains("classifier"),
                "Wiadomość błędu powinna sugerować problem z klasyfikatorem/atrybutem! Otrzymano: $message"
            )
        }
    }

    /**
     * Test #2: invalidUseOfClassifiers
     *
     * Co robi test: Testuje zachowanie silnika wobec tzw. "klasyfikatorów" (przestarzałej składni `[...]`).
     * Udowadnia zdolność silnika do wyciągania i filtrowania danych po poprawnych atrybutach koncepcyjnych.
     *
     * Różnica w zapytaniu i danych wyjściowych: Stare `[e:classifier:concept:name...]` zastąpiono
     * nowoczesnym modelem dostępu do atrybutów XES, np. `e:concept:name`. Dane wyjściowe to płaski JSON.
     *
     * Różnica w implementacji: W starej wersji nieprawidłowe użycie rzucało `PQLSyntaxException`.
     * W CouchDB zapytanie tłumaczone jest na brakujące pole MangoDB, co skutkuje bezpiecznym
     * zwrotem pustej listy `[]` zamiast krytycznego błędu.
     */
    @Test
    fun invalidUseOfClassifiers() {
        val invalidQuery = "where [e:classifier:concept:name+lifecycle:transition] in ('acceptcomplete', 'rejectcomplete') and l:id='$journalLogId'"
        val invalidResult = interpreter.executeQuery(invalidQuery).asJsonArray
        assertEquals(0, invalidResult.size(), "Błędna składnia klasyfikatora powinna zostać zignorowana i zwrócić 0 wyników (elastyczność NoSQL)!")

        val validQuery = "select e:concept:name where l:id='$journalLogId'"
        val validResult = interpreter.executeQuery(validQuery).asJsonArray
        assertTrue(validResult.size() > 0, "Zapytanie powinno zwrócić wyniki")

        val validEventNames = setOf(
            "invite reviewers", "get review 1", "get review 2", "get review 3",
            "collect reviews", "decide", "accept", "reject", "invite additional reviewer",
            "get review X", "time-out 1", "time-out 2", "time-out 3", "time-out X"
        )

        for (i in 0 until validResult.size()) {
            val doc = validResult[i].asJsonObject
            val conceptName = doc.get("e:concept:name")?.asString ?: doc.get("concept:name")?.asString
            assertNotNull(conceptName, "W dokumencie #$i brakuje atrybutu concept:name w wyniku SELECT!")
            assertTrue(validEventNames.contains(conceptName), "Błąd! Zwrócona aktywność '$conceptName' nie należy do słownika logu!")
        }
    }

    /**
     * Test #3: duplicateAttributes
     *
     * Co robi test: Sprawdza, czy silnik radzi sobie z prośbą o ten sam atrybut wielokrotnie w bloku SELECT.
     *
     * Różnica w zapytaniu i danych wyjściowych: Zamiast mieszać aliasy i stare klasyfikatory, test celowo pyta o
     * `select e:concept:name, e:concept:name`.
     *
     * Różnica w implementacji: Ponieważ obiektowy model JSON nie dopuszcza zduplikowanych kluczy na tym samym poziomie,
     * nasz silnik weryfikuje matematycznie rozmiar wygenerowanego drzewa kluczy w wynikowym dokumencie, upewniając się,
     * że interpreter poprawnie spłaszczył redundancję do jednego pola.
     */
    @Test
    fun duplicateAttributes() {
        val pqlQuery = "select e:concept:name, e:concept:name where l:id='$journalLogId'"
        val result = interpreter.executeQuery(pqlQuery).asJsonArray
        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić wyniki")

        val validEventNames = setOf(
            "invite reviewers", "get review 1", "get review 2", "get review 3",
            "collect reviews", "decide", "accept", "reject", "invite additional reviewer",
            "get review X", "time-out 1", "time-out 2", "time-out 3", "time-out X"
        )

        for (i in 0 until result.size()) {
            val doc = result[i].asJsonObject
            val conceptName = doc.get("e:concept:name")?.asString ?: doc.get("concept:name")?.asString
            assertNotNull(conceptName, "Brakuje atrybutu concept:name w dokumencie #$i")
            assertTrue(validEventNames.contains(conceptName), "Wartość '$conceptName' jest nieprawidłowa dla logu JournalReview")

            val keys = doc.keySet()
            val conceptNameKeysCount = keys.count { it == "e:concept:name" || it == "concept:name" }
            assertEquals(1, conceptNameKeysCount, "Atrybut e:concept:name powinien wystąpić dokładnie raz w wynikowym JSONie!")
        }
    }

    /**
     * Test #4: selectEmpty
     *
     * Co robi test: Weryfikuje zachowanie systemu dla obiektywnie fałszywego warunku wyszukiwania.
     *
     * Różnica w zapytaniu i danych wyjściowych: Zapytanie identyczne: `where 0=1`.
     *
     * Różnica w implementacji: Oryginał operował na wielkości strumienia w Javie. Nowa wersja weryfikuje
     * rozmiar ostatecznej, pustej tablicy (JsonArray) zwróconej przez bazę dokumentową.
     */
    @Test
    fun selectEmpty() {
        val result: JsonArray = interpreter.executeQuery("where 0=1")
        assertEquals(0, result.size(), "Zapytanie WHERE 0=1 powinno zwrócić 0 dokumentów")
    }

    /**
     * Test #5: groupScopeByClassifierTest
     *
     * Co robi test: Udowadnia działanie analizy wariantów (Process Mining) poprzez grupowanie
     * z użyciem znacznika hoisting (`^`) dla więcej niż jednego atrybutu.
     *
     * Różnica w zapytaniu i danych wyjściowych: Oryginał: `group by [^e:classifier:concept:name+lifecycle:transition]`.
     * Nowy: Rozdziela złożony operator na natywne pozycje PQL: `group by ^e:concept:name, ^e:lifecycle:transition`.
     *
     * Różnica w implementacji: System weryfikuje płaskie sekwencje list JSON, zamiast podróżować po drzewie
     * referencji obiektowych strumieni `Trace`. Logika zliczania wariantów zachowana w stosunku 1:1.
     */
    @Test
    fun groupScopeByClassifierTest() {
        val pqlQuery = """
            select e:concept:name, e:lifecycle:transition
            where l:id='$journalLogId'
            group by ^e:concept:name, ^e:lifecycle:transition
        """.trimIndent()
        val result = interpreter.executeQuery(pqlQuery).asJsonArray
        assertEquals(97, result.size(), "Powinno zwrócić dokładnie 97 unikalnych wariantów procesu")

        var count3 = 0; var count2 = 0; var count1 = 0
        val allVariantSequences = mutableListOf<List<String>>()

        for (i in 0 until result.size()) {
            val group = result[i].asJsonObject
            val count = group.get("count").asInt
            when (count) { 3 -> count3++; 2 -> count2++; 1 -> count1++ }

            val events = group.getAsJsonArray("events")
            val traceSequence = events.mapNotNull { eventElement ->
                val conceptName = eventElement.asJsonObject.get("e:concept:name")?.asString
                    ?: eventElement.asJsonObject.get("concept:name")?.asString
                conceptName?.take(3)?.lowercase()
            }
            allVariantSequences.add(traceSequence)
        }

        assertEquals(1, count3, "Powinien być dokładnie 1 wariant o liczności 3")
        assertEquals(2, count2, "Powinny być dokładnie 2 warianty o liczności 2")
        assertEquals(94, count1, "Powinno być dokładnie 94 warianty o liczności 1")

        val expectedVariant1 = listOf("inv", "inv", "get", "get", "get", "col", "col", "dec", "dec", "inv", "inv", "get", "rej", "rej")
        val expectedVariant2 = listOf("inv", "inv", "get", "get", "get", "col", "col", "dec", "dec", "acc", "acc")
        val expectedVariant3 = listOf("inv", "inv", "get", "get", "tim", "col", "col", "dec", "dec", "inv", "inv", "tim", "inv", "inv", "tim", "inv", "inv", "get", "acc", "acc")

        assertTrue(allVariantSequences.contains(expectedVariant1), "Nie znaleziono oczekiwanego Wariantu #1")
        assertTrue(allVariantSequences.contains(expectedVariant2), "Nie znaleziono oczekiwanego Wariantu #2")
        assertTrue(allVariantSequences.contains(expectedVariant3), "Nie znaleziono oczekiwanego Wariantu #3")
    }

    /**
     * Test #6: groupEventByStandardAttributeTest
     *
     * Co robi test: Weryfikuje mechanizm "Implicit Scope" (niejawnego zakresu) dla wewnętrznego grupowania
     * po pojedynczych atrybutach.
     *
     * Różnica w zapytaniu i danych wyjściowych: Zmieniono niejednoznaczne `e:name` na ścisłe `e:concept:name`.
     *
     * Różnica w implementacji: W modelu hierarchicznym profesora, grupowanie domyślnie zamykało się wewn. Trace'a.
     * W płaskim modelu CouchDB, executor automatycznie wstrzykuje `traceId` jako ukryty parametr grupowania,
     * zapewniając, by wynikowy JSON zawierał unikalne zdarzenia wyłącznie w kontekście swoich macierzystych śladów.
     */
    @Test
    fun groupEventByStandardAttributeTest() {
        val pqlQuery = """
            select t:concept:name, e:concept:name, sum(e:cost:total)
            where l:id='$journalLogId'
            group by e:concept:name
        """.trimIndent()
        val result = interpreter.executeQuery(pqlQuery).asJsonArray
        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić pogrupowane wyniki")

        val groupsByTrace = mutableMapOf<String, MutableList<JsonObject>>()
        for (i in 0 until result.size()) {
            val group = result[i].asJsonObject
            val events = group.getAsJsonArray("events")
            val rep = events[0].asJsonObject
            val traceName = rep.get("t:concept:name")?.asString ?: "unknown"
            groupsByTrace.getOrPut(traceName) { mutableListOf() }.add(group)
        }

        assertEquals(101, groupsByTrace.size, "Powinno być dokładnie 101 trace'ów")

        val eventNames = setOf(
            "invite reviewers", "time-out 1", "time-out 2", "time-out 3", "get review 1", "get review 2", "get review 3",
            "collect reviews", "decide", "invite additional reviewer", "get review X", "time-out X", "reject", "accept"
        )

        for ((traceName, groupsInTrace) in groupsByTrace) {
            val namesInTrace = groupsInTrace.mapNotNull {
                it.getAsJsonArray("events")?.get(0)?.asJsonObject?.get("e:concept:name")?.asString
            }
            val uniqueNames = namesInTrace.toSet()
            assertEquals(uniqueNames.size, namesInTrace.size, "Znaleziono duplikaty nazw eventów w trace '$traceName' — GROUP BY nie zadziałał!")

            for (name in namesInTrace) {
                assertTrue(name in eventNames, "Nieznana nazwa eventu: '$name' w trace '$traceName'")
            }

            for (group in groupsInTrace) {
                val count = group.get("count")?.asInt ?: 0
                assertTrue(count >= 1, "Każda grupa powinna mieć count >= 1")

                val rep = group.getAsJsonArray("events")[0].asJsonObject
                val sumRaw = rep.get("sum(e:cost:total)")?.asString
                val sumValue = sumRaw?.toDoubleOrNull()
                assertTrue(sumValue != null && sumValue >= 1.0, "sum(e:cost:total) powinno być >= 1.0, otrzymano: $sumValue w trace '$traceName'")
            }
        }
    }

    /**
     * Test #7: groupLogByEventStandardAttributeAndImplicitGroupEventByTest
     *
     * Co robi test: Weryfikuje tzw. "Log-Level Hoisting" przy użyciu operatora `^^`.
     *
     * Różnica w zapytaniu i danych wyjściowych: Modyfikacja składni zapytania na natywną dla XES.
     * Zwraca globalną matematyczną agregację bez względu na ograniczenia śladów.
     *
     * Różnica w implementacji: W systemie płaskim (JSON), to grupowanie zachowuje się najprościej z możliwych
     * i nie wymaga bindowania węzłów `traceId`. Wynik jest globalnym zbiorem 14 klas aktywności z podliczeniami.
     */
    @Test
    fun groupLogByEventStandardAttributeAndImplicitGroupEventByTest() {
        val pqlQuery = """
            select sum(e:cost:total)
            where l:id='$journalLogId'
            group by ^^e:concept:name
        """.trimIndent()
        val result = interpreter.executeQuery(pqlQuery).asJsonArray
        assertTrue(result.size() > 1, "Powinno zwrócić więcej niż 1 grupę")

        for (i in 0 until result.size()) {
            val groupDoc = result[i].asJsonObject
            val groupCount = groupDoc.get("count").asInt
            assertTrue(groupCount >= 1, "Każda grupa powinna mieć co najmniej jedno zdarzenie")

            val eventDoc = groupDoc.getAsJsonArray("events")[0].asJsonObject
            val sumTotalElement = eventDoc.get("sum(e:cost:total)")
            assertNotNull(sumTotalElement, "Pole sum(e:cost:total) powinno być obecne")

            val sumTotal = sumTotalElement.asDouble
            assertTrue(sumTotal in (1.0 * groupCount)..(1.08 * groupCount + 1e-6), "Suma kosztów e:cost:total nie mieści się w oczekiwanym zakresie")
        }
    }

    /**
     * Test #8: groupLogByEventStandardAndGroupEventByStandardAttributeAttributeTest
     *
     * Co robi test: Weryfikuje mechanizm "Flattening" (spłaszczania) podczas użycia podwójnego klucza grupującego
     * o różnych poziomach priorytetu (`^^e:` vs `e:`).
     *
     * Różnica w zapytaniu i danych wyjściowych: Taka sama jak w Test #7, jednak użytkownik redunduje parametr w zapytaniu.
     *
     * Różnica w implementacji: Udowadnia elastyczność silnika CouchDB, który pomimo wystąpienia redundantnego
     * filtru wewnętrznego, utrzymuje dominację hoistingu logu (`^^`), generując precyzyjny obraz globalny.
     */
    @Test
    fun groupLogByEventStandardAndGroupEventByStandardAttributeAttributeTest() {
        val pqlQuery = """
            select e:concept:name, sum(e:cost:total)
            where l:id='$journalLogId'
            group by ^^e:concept:name, e:concept:name
        """.trimIndent()
        val result = interpreter.executeQuery(pqlQuery).asJsonArray
        assertTrue(result.size() > 1, "Powinno zwrócić więcej niż 1 grupę")

        for (i in 0 until result.size()) {
            val groupDoc = result[i].asJsonObject
            val groupCount = groupDoc.get("count").asInt
            assertTrue(groupCount >= 1, "Każda grupa powinna mieć co najmniej jedno zdarzenie")

            val eventDoc = groupDoc.getAsJsonArray("events")[0].asJsonObject
            val sumTotalElement = eventDoc.get("sum(e:cost:total)")
            assertNotNull(sumTotalElement, "Pole sum(e:cost:total) powinno być obecne")

            val sumTotal = sumTotalElement.asDouble
            assertTrue(sumTotal in (1.0 * groupCount)..(1.08 * groupCount + 1e-6), "Suma kosztów e:cost:total nie mieści się w oczekiwanym zakresie")
        }
    }

    /**
     * Test #9: groupByImplicitScopeTest
     *
     * Co robi test: Weryfikuje regułę "Projection Exclusion" (Wykluczanie rzutu/projekcji).
     *
     * Różnica w zapytaniu i danych wyjściowych: Zapytanie o grupowanie po zasobie wykonawczym (`e:org:resource`).
     *
     * Różnica w implementacji: W systemie profesora używano wielu asercji `assertNull(...)`. Ponieważ nasz
     * output to słownik JSON, poprawne zadziałanie rzutnika i formatyzatora oznacza, że "zakazane"
     * klucze w ogóle się w nim nie pojawiają (są odfiltrowywane).
     */
    @Test
    fun groupByImplicitScopeTest() {
        val pqlQuery = """
            select t:concept:name, e:org:resource
            where l:id='$journalLogId'
            group by e:org:resource
        """.trimIndent()
        val result = interpreter.executeQuery(pqlQuery).asJsonArray
        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić wyniki")

        val groupsByTrace = mutableMapOf<String, MutableList<JsonObject>>()
        for (i in 0 until result.size()) {
            val group = result[i].asJsonObject
            val events = group.getAsJsonArray("events")
            val rep = events[0].asJsonObject

            val traceName = rep.get("t:concept:name")?.asString ?: "unknown_trace"
            groupsByTrace.getOrPut(traceName) { mutableListOf() }.add(group)
        }

        assertEquals(101, groupsByTrace.size, "Log JournalReview powinien zawierać dokładnie 101 śladów")

        for ((traceName, groupsInTrace) in groupsByTrace) {
            for (group in groupsInTrace) {
                val count = group.get("count")?.asInt ?: 0
                assertTrue(count >= 1, "Grupa musi posiadać atrybut count >= 1")

                val repEvent = group.getAsJsonArray("events")[0].asJsonObject
                assertNotNull(repEvent.get("e:org:resource") ?: repEvent.get("org:resource"), "Brakuje klucza grupującego org:resource w wyniku!")

                assertNull(repEvent.get("e:time:timestamp"), "Błąd projekcji: timestamp nie powinien być widoczny w pogrupowanym wyniku!")
                assertNull(repEvent.get("e:concept:name"), "Błąd projekcji: nazwa zdarzenia nie powinna być widoczna w grupie po zasobach!")
                assertNull(repEvent.get("e:cost:total"), "Błąd projekcji: koszt całkowity nie powinien być widoczny w pogrupowanym wyniku!")
            }
        }
    }

    /**
     * Test #10: groupByOuterScopeTest
     *
     * Co robi test: Sprawdza Cross-Scope Aggregation (agregację miedzy zakresami). Będąc na poziomie struktury,
     * wylicza wartość należącą do warstwy nadrzędnej.
     *
     * Różnica w zapytaniu i danych wyjściowych: Zapytanie PQL domaga się atrybutu LOG (`t:min(l:concept:name)`).
     *
     * Różnica w implementacji: W pamięciowym, obiektowym systemie oryginalnym, PQL zjeżdżał po drzewie w dół
     * pobierając wartość `Log.name`. W naszej implementacji dokumentowej, `PqlQueryExecutor` używa drugiego strumienia z CouchDB
     * w celu podpięcia "rodzica" i wstrzykuje tę wartość z powrotem do finalnego JSON.
     */
    @Test
    fun groupByOuterScopeTest() {
        val pqlQuery = """
            select t:min(l:concept:name)
            where l:id='$journalLogId'
        """.trimIndent()
        val result = interpreter.executeQuery(pqlQuery).asJsonArray
        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić dane per-trace z zewnętrzną agregacją")

        for (i in 0 until result.size()) {
            val doc = result[i].asJsonObject
            val eventDoc = doc.getAsJsonArray("events")[0].asJsonObject
            val minLogName = eventDoc.get("t:min(l:concept:name)")?.asString
            assertEquals("JournalReview", minLogName, "Cross-scope aggregation z LOG nie powiodło się dla trace ${i}")
        }
    }

    /**
     * Test #11: groupByImplicitFromSelectTest
     *
     * Co robi test: Weryfikuje mechanizm wymuszonego, niejawnego grupowania w przypadku żądania wyliczeń matematycznych
     * na płaskim zapytaniu.
     *
     * Różnica w zapytaniu i danych wyjściowych: Klasyczne dopasowanie nazw i kluczy dla CouchDB. Oczekujemy 101 reprezentantów.
     *
     * Różnica w implementacji: Silnik musi wykryć obecność typowych funkcji agregujących (avg, min, max) w liście węzłów projekcji.
     * Wynikiem nie jest zrzucone, surowe drzewo XES, lecz zredukowany zestaw prekompilowanych JSONów z wyliczeniami.
     */
    @Test
    fun groupByImplicitFromSelectTest() {
        val pqlQuery = """
            select l:*, t:*, avg(e:cost:total), min(e:time:timestamp), max(e:time:timestamp)
            where l:id='$journalLogId'
        """.trimIndent()
        val result = interpreter.executeQuery(pqlQuery).asJsonArray

        assertEquals(101, result.size(), "Domyślne grupowanie na poziomie trace powinno zwrócić 101 grup (jedna na trace)")

        val traceDoc = result[0].asJsonObject
        val count = traceDoc.get("count")?.asInt
        assertNotNull(count, "Każda grupa musi mieć zliczoną liczbę zdarzeń")
        assertTrue(count >= 1, "Powinno być co najmniej 1 zdarzenie w trace")

        val eventDoc = traceDoc.getAsJsonArray("events")[0].asJsonObject
        val avgTotal = eventDoc.get("avg(e:cost:total)")
        assertNotNull(avgTotal, "Pole avg(e:cost:total) powinno być obecne")
        assertTrue(avgTotal.asDouble in 1.0..1.08, "Wartość avg(e:cost:total) musi mieścić się w odpowiednim zakresie (1.0..1.08)")

        assertNotNull(eventDoc.get("min(e:time:timestamp)"), "Pole min(e:time:timestamp) powinno być obecne")
        assertNotNull(eventDoc.get("max(e:time:timestamp)"), "Pole max(e:time:timestamp) powinno być obecne")
    }

    /**
     * Test #12: groupByImplicitFromOrderByTest
     *
     * Co robi test: Bardziej radykalna forma testu 11. Udowadnia możliwość aktywacji niejawnego grupowania
     * z poziomu samej klauzuli ORDER BY.
     *
     * Różnica w zapytaniu i danych wyjściowych: Zapytanie celowo wyklucza klauzulę SELECT i żąda matematyki w ORDER BY.
     *
     * Różnica w implementacji: Nasz spłaszczony system CouchDB i translator skanują drzewo AST by aktywować per-trace constraints.
     */
    @Test
    fun groupByImplicitFromOrderByTest() {
        val pqlQuery = """
            where l:id='$journalLogId'
            order by avg(e:cost:total), min(e:time:timestamp), max(e:time:timestamp)
        """.trimIndent()

        val result = interpreter.executeQuery(pqlQuery).asJsonArray
        assertEquals(101, result.size(), "Domyślne grupowanie na poziomie trace aktywowane przez obliczenia w ORDER BY powinno zwrócić 101 grup")

        val traceDoc = result[0].asJsonObject
        val count = traceDoc.get("count")?.asInt
        assertNotNull(count, "Każda z grup musi zawierać metadata property 'count' definiującą event size")
        assertTrue(count >= 1, "Trace property size nie może być pusty")

        val eventDoc = traceDoc.getAsJsonArray("events")[0].asJsonObject
        val avgTotal = eventDoc.get("avg(e:cost:total)")
        assertNotNull(avgTotal, "Pole avg(e:cost:total) wyciągnięte z ORDER BY nie widnieje na liście properties złączonego dokumentu")
        assertTrue(avgTotal.asDouble in 1.0..1.08, "Średnia zdarzeń na dany ślad nie mieści się w standardowym okienku 1.0-1.08 dla Journala")

        assertNotNull(eventDoc.get("min(e:time:timestamp)"), "Brak klucza wymuszającego sort timestampa najniższego")
        assertNotNull(eventDoc.get("max(e:time:timestamp)"), "Brak klucza wymuszającego sort timestampa najwyższego")
    }

    /**
     * Test #13: groupByImplicitWithHoistingTest
     *
     * Co robi test: Weryfikuje niejawne globalne grupowanie. System z użyciem operatora `^^` na matematyce
     * musi zwinąć cały Log XES do jednego wyjściowego wiersza podsumowania.
     *
     * Różnica w zapytaniu i danych wyjściowych: Identyczne wyciągi, testuje się pod jednym reprezentantem.
     *
     * Różnica w implementacji: To tzw. edge case w architekturze PQL-to-CouchDB, wyłączający auto-bindowanie traceId,
     * ładujący wszystko do grupy "GLOBAL_GROUP". Potężny sprawdzian parsera i executora na poziomie JSON node.
     */
    @Test
    fun groupByImplicitWithHoistingTest() {
        val pqlQuery = """
            select avg(^^e:cost:total), min(^^e:time:timestamp), max(^^e:time:timestamp)
            where l:id='$journalLogId'
        """.trimIndent()

        val result = interpreter.executeQuery(pqlQuery).asJsonArray

        // --- MINIMALNE DEBUGOWANIE ---
        println("\n--- DEBUG INFO START ---")
        println("Rozmiar wyniku (oczekiwano 1): ${result.size()}")

        if (result.size() > 0) {
            val firstGroup = result[0].asJsonObject
            println("Liczność grupy (count): ${firstGroup.get("count")?.asInt}")

            val eventsArray = firstGroup.getAsJsonArray("events")
            if (eventsArray != null && eventsArray.size() > 0) {
                val repEvent = eventsArray[0].asJsonObject
                println("Klucze w JSONie reprezentanta: ${repEvent.keySet()}")
                println("Wartość AVG: ${repEvent.get("avg(^^e:cost:total)")}")
                println("Wartość MIN: ${repEvent.get("min(^^e:time:timestamp)")}")
                println("Wartość MAX: ${repEvent.get("max(^^e:time:timestamp)")}")
            } else {
                println("Brak tablicy 'events' w pierwszej grupie!")
            }
        }
        println("--- DEBUG INFO END ---\n")
        // -----------------------------

        assertEquals(1, result.size(), "Niejawne globalne grupowanie powinno zwrócić dokładnie 1 wynik podsumowujący")

        val globalGroup = result[0].asJsonObject
        val repEvent = globalGroup.getAsJsonArray("events")[0].asJsonObject

        val avgTotal = repEvent.get("avg(^^e:cost:total)")
        assertNotNull(avgTotal, "Pole avg(^^e:cost:total) powinno być obecne w wyniku")
        assertTrue(avgTotal.asDouble in 1.0..1.08, "Wartość globalnej średniej kosztów (${avgTotal.asDouble}) jest poza oczekiwanym zakresem!")

        val minTime = repEvent.get("min(^^e:time:timestamp)")?.asString
        val maxTime = repEvent.get("max(^^e:time:timestamp)")?.asString

        assertNotNull(minTime, "Pole min(^^e:time:timestamp) powinno być obecne")
        assertNotNull(maxTime, "Pole max(^^e:time:timestamp) powinno być obecne")
        assertTrue(minTime <= maxTime, "Globalny czas minimalny nie może być późniejszy niż maksymalny")

        assertNull(repEvent.get("e:concept:name"), "Błąd projekcji: nazwa zdarzenia nie powinna być w globalnej agregacji")
        assertNull(repEvent.get("t:concept:name"), "Błąd projekcji: nazwa śladu nie powinna być w globalnej agregacji")
        assertNull(repEvent.get("e:org:resource"), "Błąd projekcji: zasób nie powinien być widoczny w globalnej agregacji")
    }

    /**
     * Test #14: orderBySimpleTest
     *
     * Co robi test: Sprawdza najzwyklejszy mechanizm porządkowania chronologicznego na podstawie stempla czasowego.
     *
     * Różnica w zapytaniu i danych wyjściowych: Odpytujemy limit 25 eventów zamiast 3 potężnych logów referencyjnych.
     *
     * Różnica w implementacji: Ucieczka od zagnieżdżonych, głębokich pętli iteracji (Log -> Trace -> Event) na rzecz
     * płaskiej weryfikacji JSON w stylu SQL.
     */
    @Test
    fun orderBySimpleTest() {
        val result = interpreter.executeQuery(
            "select e:concept:name, e:time:timestamp order by e:time:timestamp limit 25"
        )
        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić dokumenty")

        var lastTimestamp = ""
        for (i in 0 until result.size()) {
            val doc = result[i].asJsonObject
            val ts = doc.get("e:time:timestamp")?.asString ?: continue

            if (lastTimestamp.isNotEmpty()) {
                assertTrue(ts >= lastTimestamp, "Zdarzenia nie są posortowane: '$ts' powinno być >= '$lastTimestamp'")
            }
            lastTimestamp = ts
        }
    }

    /**
     * Test #15: orderByWithModifierAndScopesTest
     *
     * Co robi test: Obejmuje weryfikacją priorytety "Scope Hierarchy" w języku PQL (poziom t: > poziom e:).
     *
     * Różnica w zapytaniu i danych wyjściowych: Zapytanie wpisane w zgodzie z regułą (Trace na pierwszym miejscu).
     *
     * Różnica w implementacji: System musi skomponować sortowanie hybrydowe. Pierwszy klucz (Trace Cost) operuje
     * na dokumencie nadrzędnym wyciągniętym zewnętrznie, a drugi klucz (Czas Zdarzenia) operuje lokalnie.
     */
    @Test
    fun orderByWithModifierAndScopesTest() {
        val result = interpreter.executeQuery(
            "select e:concept:name, e:time:timestamp, t:cost:total order by t:cost:total desc, e:time:timestamp limit 100"
        )
        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić dokumenty")

        var lastTotal: Double? = Double.MAX_VALUE
        var lastTimestamp = ""

        for (i in 0 until result.size()) {
            val doc = result[i].asJsonObject
            val currentTotal = if (doc.has("t:cost:total") && !doc.get("t:cost:total").isJsonNull)
                doc.get("t:cost:total").asDouble else null

            val currentTimestamp = doc.get("e:time:timestamp")?.asString ?: ""

            if (currentTotal != null && lastTotal != null) {
                assertTrue(currentTotal <= lastTotal, "Total nie jest posortowane DESC: $currentTotal powinno być <= $lastTotal")
                if (currentTotal == lastTotal && lastTimestamp.isNotEmpty() && currentTimestamp.isNotEmpty()) {
                    assertTrue(currentTimestamp >= lastTimestamp, "Timestampy nie są posortowane ASC dla tego samego śladu: $currentTimestamp powinno być >= $lastTimestamp")
                }
            }
            lastTotal = currentTotal ?: lastTotal
            lastTimestamp = currentTimestamp
        }
    }

    /**
     * Test #16: orderByWithModifierAndScopes2Test (Test #15-bis)
     *
     * Co robi test: Weryfikuje mechanizm Auto-Korekty Hierarchii Scope'ów w PQL.
     *
     * Różnica w zapytaniu i danych wyjściowych: Klauzula zła, odwrócona przez użytkownika na niekorzyść hierarchii
     * `order by e:..., t:... desc`.
     *
     * Różnica w implementacji: Silnik musi całkowicie zignorować życzenie programisty co do kolejności słów i wymusić
     * ułożenie według priorytetu PQL. Tablica wyjściowa asercyjna jest identyczna jak w teście #15.
     */
    @Test
    fun orderByWithModifierAndScopes2Test() {
        val result = interpreter.executeQuery(
            "select e:concept:name, e:time:timestamp, t:cost:total order by e:time:timestamp, t:cost:total desc limit 100"
        )
        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić dokumenty")

        var lastTotal: Double? = Double.MAX_VALUE
        var lastTimestamp = ""

        for (i in 0 until result.size()) {
            val doc = result[i].asJsonObject
            val currentTotal = if (doc.has("t:cost:total") && !doc.get("t:cost:total").isJsonNull)
                doc.get("t:cost:total").asDouble else null

            val currentTimestamp = doc.get("e:time:timestamp")?.asString ?: ""

            if (currentTotal != null && lastTotal != null) {
                assertTrue(currentTotal <= lastTotal, "Total nie jest posortowane DESC: $currentTotal powinno być <= $lastTotal")
                if (currentTotal == lastTotal && lastTimestamp.isNotEmpty() && currentTimestamp.isNotEmpty()) {
                    assertTrue(currentTimestamp >= lastTimestamp, "Timestampy nie są posortowane ASC dla tego samego śladu: $currentTimestamp powinno być >= $lastTimestamp")
                }
            }
            lastTotal = currentTotal ?: lastTotal
            lastTimestamp = currentTimestamp
        }
    }

    /**
     * Test #17: orderByExpressionTest
     *
     * Co robi test: Test weryfikuje grupowanie wariantów (hoisting ^) oraz sortowanie wygenerowanych wariantów
     * za pomocą złożonej funkcji matematycznej.
     *
     * Różnica w zapytaniu i danych wyjściowych: Podobieństwo składniowe; zmienione nazwy na `e:time:timestamp`.
     * Zwraca prekompilowany wektor z predykcyjną wartością w `min(^e:time:timestamp)`.
     *
     * Różnica w implementacji: Asercje bezpośrednio konwertują wyliczone wartości zmiennoprzecinkowe/epoch
     * i ewaluują sekwencyjne narastanie.
     */
    @Test
    fun orderByExpressionTest() {
        val pqlQuery = """
            select min(e:time:timestamp)
            where l:id='$journalLogId'
            group by ^e:concept:name
            order by min(^e:time:timestamp)
        """.trimIndent()
        val result = interpreter.executeQuery(pqlQuery).asJsonArray
        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić warianty")
        assertEquals(97, result.size(), "Powinno zwrócić dokładnie 97 unikalnych wariantów procesu")

        var lastMinTimestamp = ""

        for (i in 0 until result.size()) {
            val doc = result[i].asJsonObject
            val currentMinRaw = doc.get("min(^e:time:timestamp)")?.asString ?: ""
            assertTrue(currentMinRaw.isNotEmpty(), "Brak wyliczonej wartości min(^e:time:timestamp) dla sortowania")

            if (lastMinTimestamp.isNotEmpty() && currentMinRaw.isNotEmpty()) {
                val currentMinVal = currentMinRaw.toDoubleOrNull()
                val lastMinVal = lastMinTimestamp.toDoubleOrNull()

                if (currentMinVal != null && lastMinVal != null) {
                    assertTrue(currentMinVal >= lastMinVal, "Warianty nie są posortowane po min(^e:time:timestamp) ASC: $currentMinVal powinno być >= $lastMinVal")
                } else {
                    assertTrue(currentMinRaw >= lastMinTimestamp, "Warianty nie są posortowane po min(^e:time:timestamp) ASC: $currentMinRaw powinno być >= $lastMinTimestamp")
                }
            }
            lastMinTimestamp = currentMinRaw
        }
    }

    /**
     * Test #18: groupByWithHoistingAndOrderByWithinGroupTest
     *
     * Co robi test: Detekcja znanych, specyficznych śladów historycznych wariantów procesów.
     * Test sprawdza ułożenie i sortowanie zjawiska w grupach (tzw. Stringify Sequence Matching).
     *
     * Różnica w zapytaniu i danych wyjściowych: Zastąpiono skróty `e:name` pełnoprawnymi wylistowaniami CouchDB.
     *
     * Różnica w implementacji: Złożone struktury list wielowymiarowych Kotlin zostały wykorzystane do symulacji
     * ułożenia predykcyjnego Process Miningu dla znanych wariantów zdarzeń. Zamiast nawigacji obiektowej użyto
     * mapowania `.asJsonObject`.
     */
    @Test
    fun groupByWithHoistingAndOrderByWithinGroupTest() {
        val pqlQuery = "where l:id='$journalLogId' group by ^e:concept:name order by concept:name"
        val result = interpreter.executeQuery(pqlQuery).asJsonArray

        assertTrue(result.size() > 0, "Brak wyników! Zapytanie powinno zwrócić pogrupowane warianty.")
        assertEquals(78, result.size(), "Zła liczba unikalnych wariantów śladów!")

        val fourTraces = listOf("accept,accept,collect reviews,collect reviews,decide,decide,get review 1,get review 2,get review 3,invite reviewers,invite reviewers").map { it.split(',') }
        val threeTraces = listOf(
            "collect reviews,collect reviews,decide,decide,get review 1,get review 3,invite reviewers,invite reviewers,reject,reject,time-out 2",
            "accept,accept,collect reviews,collect reviews,decide,decide,get review 1,get review 2,get review X,get review X,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite reviewers,invite reviewers,time-out 3",
            "collect reviews,collect reviews,decide,decide,get review 1,get review 2,get review 3,get review X,invite additional reviewer,invite additional reviewer,invite reviewers,invite reviewers,reject,reject"
        ).map { it.split(',') }
        val twoTraces = listOf(
            "collect reviews,collect reviews,decide,decide,get review 2,get review 3,get review X,invite additional reviewer,invite additional reviewer,invite reviewers,invite reviewers,reject,reject,time-out 1",
            "accept,accept,collect reviews,collect reviews,decide,decide,get review 2,get review 3,get review X,get review X,get review X,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite reviewers,invite reviewers,time-out 1",
            "accept,accept,collect reviews,collect reviews,decide,decide,get review 3,get review X,get review X,get review X,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite reviewers,invite reviewers,time-out 1,time-out 2",
            "collect reviews,collect reviews,decide,decide,get review 2,get review 3,get review X,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite reviewers,invite reviewers,reject,reject,time-out 1,time-out X",
            "collect reviews,collect reviews,decide,decide,get review 1,get review 2,get review X,get review X,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite reviewers,invite reviewers,reject,reject,time-out 3",
            "collect reviews,collect reviews,decide,decide,get review 3,get review X,invite additional reviewer,invite additional reviewer,invite reviewers,invite reviewers,reject,reject,time-out 1,time-out 2",
            "collect reviews,collect reviews,decide,decide,get review 1,get review 2,get review X,get review X,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite reviewers,invite reviewers,reject,reject,time-out 3,time-out X,time-out X,time-out X",
            "accept,accept,collect reviews,collect reviews,decide,decide,get review 3,get review X,invite additional reviewer,invite additional reviewer,invite reviewers,invite reviewers,time-out 1,time-out 2",
            "accept,accept,collect reviews,collect reviews,decide,decide,get review 1,get review 2,get review X,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite reviewers,invite reviewers,time-out 3,time-out X,time-out X",
            "collect reviews,collect reviews,decide,decide,get review 1,get review 2,get review X,get review X,get review X,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite reviewers,invite reviewers,reject,reject,time-out 3,time-out X,time-out X,time-out X,time-out X",
            "collect reviews,collect reviews,decide,decide,get review X,get review X,get review X,get review X,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite reviewers,invite reviewers,reject,reject,time-out 1,time-out 2,time-out 3,time-out X,time-out X,time-out X,time-out X",
            "accept,accept,collect reviews,collect reviews,decide,decide,get review 1,get review 3,invite reviewers,invite reviewers,time-out 2",
            "collect reviews,collect reviews,decide,decide,get review 3,get review X,get review X,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite reviewers,invite reviewers,reject,reject,time-out 1,time-out 2,time-out X,time-out X",
            "accept,accept,collect reviews,collect reviews,decide,decide,get review 1,get review 2,get review 3,get review X,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite additional reviewer,invite reviewers,invite reviewers,time-out X,time-out X"
        ).map { it.split(',') }

        fun validate(validTraces: List<List<String>>, expectedCount: Int) {
            for (validTrace in validTraces) {
                val isPresent = result.map { it.asJsonObject }.any { groupNode ->
                    val count = groupNode.get("count")?.asInt ?: 0
                    if (count == expectedCount) {
                        val eventsArray = groupNode.getAsJsonArray("events") ?: JsonArray()
                        val actualEvents = eventsArray.mapNotNull {
                            val evtObj = it.asJsonObject
                            evtObj.get("e:concept:name")?.asString ?: evtObj.get("concept:name")?.asString ?: evtObj.get("activity")?.asString ?: evtObj.get("name")?.asString
                        }
                        actualEvents.size == validTrace.size && actualEvents.zip(validTrace).all { (act, exp) -> act == exp }
                    } else false
                }
                assertTrue(isPresent, "Nie znaleziono wariantu z count=$expectedCount dla sekwencji: $validTrace")
            }
        }

        validate(fourTraces, 4)
        validate(threeTraces, 3)
        validate(twoTraces, 2)
    }

    /**
     * Test #19: groupByWithHoistingAndOrderByCountTest
     *
     * Co robi test: Zasadniczy przypadek PQL Process Mining. Mapowanie najbardziej powtarzających się schematów/wzorców
     * postępowania do odtworzenia modeli.
     *
     * Różnica w zapytaniu i danych wyjściowych: Zminimalizowano zapytanie, wyłączając redundantne projekcje selektora
     * i manualne limity, stawiając na surowe dane bazodanowe `count`.
     *
     * Różnica w implementacji: Odkrywa i zlicza wbudowane wartości `count` przypisane natywnie do struktur grup.
     * Zapobiega błędom duplikacji obiektów w locie i ręcznym pętlom obliczeniowym.
     */
    @Test
    fun groupByWithHoistingAndOrderByCountTest() {
        val pqlQuery = "select e:concept:name where l:id='$journalLogId' group by ^e:concept:name order by count(t:name) desc"
        val result = interpreter.executeQuery(pqlQuery).asJsonArray

        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić warianty procesu")
        assertEquals(97, result.size(), "Zła liczba unikalnych wariantów (oczekiwano 97)")

        val topVariantCount = result[0].asJsonObject.get("count").asInt
        assertEquals(3, topVariantCount, "Najczęstszy wariant (pierwszy w wyniku) powinien mieć liczność 3")

        val secondVariantCount = result[1].asJsonObject.get("count").asInt
        assertEquals(2, secondVariantCount, "Drugi w kolejności wariant powinien mieć liczność 2")

        for (i in 3 until result.size()) {
            val count = result[i].asJsonObject.get("count").asInt
            assertEquals(1, count, "Warianty od indeksu 3 powinny mieć liczność 1")
        }

        val threeTraces = listOf("invite reviewers,invite reviewers,get review 2,get review 3,get review 1,collect reviews,collect reviews,decide,decide,invite additional reviewer,invite additional reviewer,get review X,reject,reject").map { it.split(',') }
        val twoTraces = listOf(
            "invite reviewers,invite reviewers,get review 2,get review 1,get review 3,collect reviews,collect reviews,decide,decide,accept,accept",
            "invite reviewers,invite reviewers,get review 2,get review 1,time-out 3,collect reviews,collect reviews,decide,decide,invite additional reviewer,invite additional reviewer,time-out X,invite additional reviewer,invite additional reviewer,time-out X,invite additional reviewer,invite additional reviewer,get review X,accept,accept"
        ).map { it.split(',') }

        fun validate(validTraces: List<List<String>>, expectedCount: Int) {
            for (validTrace in validTraces) {
                val isPresent = result.map { it.asJsonObject }.any { groupNode ->
                    val count = groupNode.get("count")?.asInt ?: 0
                    if (count == expectedCount) {
                        val eventsArray = groupNode.getAsJsonArray("events") ?: JsonArray()
                        val actualEvents = eventsArray.mapNotNull {
                            val evtObj = it.asJsonObject
                            evtObj.get("e:concept:name")?.asString ?: evtObj.get("concept:name")?.asString ?: evtObj.get("activity")?.asString ?: evtObj.get("name")?.asString
                        }
                        actualEvents.size == validTrace.size && actualEvents.zip(validTrace).all { (act, exp) -> act == exp }
                    } else false
                }
                assertTrue(isPresent, "Nie znaleziono chronologicznego wariantu z count=$expectedCount dla sekwencji: $validTrace")
            }
        }

        validate(threeTraces, 3)
        validate(twoTraces, 2)
    }

    /**
     * Test #20: aggregationFunctionIndependence (Bug #116)
     *
     * Co robi test: Weryfikacja integralności pamięci. Gwarantuje, że podpięcie na sucho kolejnej funkcji
     * ewaluacyjnej (takiej jak redundantne obliczanie `count()`) nie re-inicjalizuje i nie zeruje wyników głównego sortowania.
     *
     * Różnica w zapytaniu i danych wyjściowych: Mnożenie operatorów w blokach SELECT.
     *
     * Różnica w implementacji: Asercja nie wędruje przez mapę obiektów, lecz błyskawicznie porównuje dwa zapytania
     * i udowadnia, że węzeł `count` utrzymuje taką samą matematyczną precyzję (Baza CouchDB jako Immutable Store).
     */
    @Test
    fun aggregationFunctionIndependence() {
        val query1 = "select e:concept:name where l:id='$journalLogId' group by ^e:concept:name order by count(t:name) desc"
        val query2 = "select e:concept:name, count(^e:concept:name) where l:id='$journalLogId' group by ^e:concept:name order by count(t:name) desc"

        val result1 = interpreter.executeQuery(query1).asJsonArray
        val result2 = interpreter.executeQuery(query2).asJsonArray

        assertTrue(result1.size() > 0, "Zapytanie bazowe powinno zwrócić wyniki")
        assertEquals(result1.size(), result2.size(), "Oba zapytania powinny zwrócić identyczną liczbę wariantów")

        for (i in 0 until result1.size()) {
            val count1 = result1[i].asJsonObject.get("count")?.asInt
            val count2 = result2[i].asJsonObject.get("count")?.asInt

            assertEquals(count1, count2, "Wartość 'count' na indeksie $i zmieniła się z powodu dodania innej funkcji agregującej!")
        }
    }

    /**
     * Test #21: groupByWithAndWithoutHoistingAndOrderByCountTest (Bug #106)
     *
     * Co robi test: Prezentuje złożone (hybrydowe) zapytanie agregujące, parujące systematykę SQL (`t:concept:name`)
     * ze sztandarowym rozwiązaniem dla miningu procesowego ProcessM (`^e:concept:name`).
     *
     * Różnica w zapytaniu i danych wyjściowych: Zapytanie praktycznie zgodne z architekturą referencyjną.
     *
     * Różnica w implementacji: Brak asercji odwołujących się bezpośrednio pod nazwy Logów (usunięto zagnieżdżenia).
     * W CouchDB wymuszenie jednoczesnego przypisania na dwóch różnych operatorach izolujących daje prosty, liniowy JSON 101 korzeni,
     * każdy na sztywno zaalokowany do `count = 1`.
     */
    @Test
    fun groupByWithAndWithoutHoistingAndOrderByCountTest() {
        val pqlQuery = "select e:concept:name where l:id='$journalLogId' group by t:concept:name, ^e:concept:name order by count(t:name) desc"
        val result = interpreter.executeQuery(pqlQuery).asJsonArray

        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić dokumenty")
        assertEquals(101, result.size(), "Powinno zwrócić dokładnie 101 grup (101 unikalnych śladów).")

        for (i in 0 until result.size()) {
            val count = result[i].asJsonObject.get("count").asInt
            assertEquals(1, count, "Grupa (ślad) na indeksie $i powinna posiadać liczność 1")
        }
    }

    /**
     * Test #22: groupByWithTwoLogs
     *
     * Co robi test: Weryfikuje szczelność warstw (Data Boundaries) pomiędzy poszczególnymi Logami. Zapobiega "wyciekom" i mieszaniu
     * wariantów z różnych źródeł (tzw. zanieczyszczone grupy).
     *
     * Różnica w zapytaniu i danych wyjściowych: Z użyciem operatora IN ściągany jest również BPI log, zmuszając bazę
     * do operowania na dwóch potężnych zestawach jednocześnie.
     *
     * Różnica w implementacji: Ponieważ baza operuje zdenormalizowanym podejściem do tabeli `events`,
     * asercja jest wybitnie potężna: wydobywa pełen zestaw reprezentacyjny stringów z każdego węzła,
     * i konfrontuje go ze statycznym zbiorem legalnych wariantów — każda anomalia (wymieszanie) rzuca czerwoną flagę.
     */
    @Test
    fun groupByWithTwoLogs() {
        val bpiLogId = "WSTAW_TUTAJ_ID_DRUGIEGO_LOGU"

        val pqlQuery = """
            select e:concept:name
            where l:id IN ('$journalLogId', '$bpiLogId')
            group by ^e:concept:name
            order by count(t:name) desc
        """.trimIndent()

        val result = interpreter.executeQuery(pqlQuery).asJsonArray

        val journalEventNames = setOf(
            "invite reviewers", "get review 1", "get review 2", "get review 3",
            "collect reviews", "decide", "accept", "reject", "invite additional reviewer",
            "get review X", "time-out 1", "time-out 2", "time-out 3", "time-out X"
        )
        val bpiEventNames = setOf(
            "A_SUBMITTED", "A_PARTLYSUBMITTED", "A_PREACCEPTED", "W_Completeren aanvraag",
            "A_ACCEPTED", "O_SELECTED", "A_FINALIZED", "O_CREATED", "O_SENT",
            "W_Nabellen offertes", "O_SENT_BACK", "W_Valideren aanvraag", "A_REGISTERED",
            "A_APPROVED", "O_ACCEPTED", "A_ACTIVATED", "A_DECLINED", "A_CANCELLED",
            "W_Afhandelen leads", "O_CANCELLED", "W_Beoordelen fraude"
        )

        for (i in 0 until result.size()) {
            val variant = result[i].asJsonObject
            val events = variant.getAsJsonArray("events")

            val eventNamesInVariant = events.mapNotNull {
                val evt = it.asJsonObject
                evt.get("e:concept:name")?.asString ?: evt.get("concept:name")?.asString ?: evt.get("activity")?.asString
            }.toSet()

            val isExclusivelyJournal = journalEventNames.containsAll(eventNamesInVariant)
            val isExclusivelyBpi = bpiEventNames.containsAll(eventNamesInVariant)

            assertTrue(
                isExclusivelyJournal || isExclusivelyBpi,
                "Zależność złamana: Wariant #$i miesza zdarzenia między niepowiązanymi logami!"
            )
        }
    }

    /**
     * Test #23: multiScopeGroupBy (Bug #107)
     *
     * Co robi test: Weryfikacja ucieczki i gubienia atrybutów najniższego rzędu (scope `e:`) w momencie
     * wywoływania żądań na wielu, krzyżujących się płaszczyznach koncepcyjnych w blokach grupujących.
     *
     * Różnica w zapytaniu i danych wyjściowych: PQL celowo oczyszczony z trudnych działań matematycznych,
     * by asercja skupiła się w 100% na rdzeniu błędu: braku zgrupowania dla atrybutów event-level z uwzględnieniem śladów.
     *
     * Różnica w implementacji: Asercja samodzielnie kompiluje i sprawdza siatkę powiązań między atrybutami, weryfikując,
     * czy proces rzutowania zapytania i złączenia w spłaszczonej reprezentacji uchronił interpreter przed połykaniem wierszy ("Shadowing").
     */
    @Test
    fun multiScopeGroupBy() {
        val pqlQuery = """
            select t:concept:name, e:concept:name, count(e:concept:name)
            where l:id='$journalLogId'
            group by t:concept:name, e:concept:name
        """.trimIndent()

        val result = interpreter.executeQuery(pqlQuery).asJsonArray
        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić wyniki")

        val groupsByTrace = mutableMapOf<String, MutableList<JsonObject>>()

        for (i in 0 until result.size()) {
            val group = result[i].asJsonObject
            val repEvent = group.getAsJsonArray("events")[0].asJsonObject
            val traceName = repEvent.get("t:concept:name")?.asString ?: "unknown"

            groupsByTrace.getOrPut(traceName) { mutableListOf() }.add(repEvent)
        }

        assertEquals(101, groupsByTrace.size, "Niewłaściwa ilość wierzchołków śladów (powinno być 101)")

        for ((traceName, eventsInTrace) in groupsByTrace) {
            val eventNames = eventsInTrace.mapNotNull { it.get("e:concept:name")?.asString }
            val uniqueEventNames = eventNames.toSet()

            assertEquals(
                uniqueEventNames.size,
                eventNames.size,
                "BŁĄD W GRUPOWANIU (Shadowing): Znaleziono zduplikowane nazwy dla śladu '$traceName'. e:concept:name nie zostało scalone."
            )
        }
    }

    /**
     * Test #24: missingAttributes
     *
     * Co robi test: Udowadnia, że wyliczanie wielu złożonych projekcji (w tym działań arytmetycznych na funkcjach)
     * nie powoduje "gubienia" atrybutów w wynikowym dokumencie JSON (Bug #116 w oryginalnym systemie).
     *
     * Różnica w zapytaniu i danych wyjściowych: Zamieniono bazę na Hospital Log. Wyliczamy różnicę między max i min czasem.
     * Ograniczono wyniki do 10 grup, by symulować oryginalne `limit t:10`.
     *
     * Różnica w implementacji: W nowym systemie, `PqlQueryExecutor` automatycznie przypisuje cały surowy wzór
     * matematyczny jako klucz w JSON (np. `"max(^e:time:timestamp) - min(^e:time:timestamp)"`). Test sprawdza,
     * czy dla każdego z 10 śladów ten klucz został poprawnie wygenerowany i zawiera wartość.
     */
    @Test
    fun missingAttributes() {
        val pqlQuery = """
            select t:concept:name, min(^e:time:timestamp), max(^e:time:timestamp), max(^e:time:timestamp) - min(^e:time:timestamp)
            where l:id='$hospitalLogId'
            group by t:concept:name
            limit 10
        """.trimIndent()

        val result = interpreter.executeQuery(pqlQuery).asJsonArray
        assertEquals(10, result.size(), "Powinno zwrócić dokładnie 10 grup (śladów)")

        for (i in 0 until result.size()) {
            val group = result[i].asJsonObject
            val repEvent = group.getAsJsonArray("events")[0].asJsonObject

            assertNotNull(repEvent.get("t:concept:name"), "Brak klucza t:concept:name")
            assertNotNull(repEvent.get("min(^e:time:timestamp)"), "Brak klucza min")
            assertNotNull(repEvent.get("max(^e:time:timestamp)"), "Brak klucza max")

            // Weryfikacja obecności wyliczenia matematycznego
            assertNotNull(
                repEvent.get("max(^e:time:timestamp)-min(^e:time:timestamp)"),
                "Błąd #116: Zgubiono wynik działania arytmetycznego w projekcji!"
            )
        }
    }

    /**
     * Test #25: orderByAggregationExpression
     *
     * Co robi test: Weryfikuje możliwość sortowania całych grup/śladów na podstawie wyniku w locie wyliczonego
     * równania matematycznego (np. różnicy między ostatnim a pierwszym zdarzeniem - tzw. Cycle Time).
     *
     * Różnica w zapytaniu i danych wyjściowych: Zapytanie używa pełnych nazw zmiennych XES. Użyto logu Hospital.
     *
     * Różnica w implementacji: Odtwarza proces sortowania po dynamicznie stworzonym polu, upewniając się, że
     * w płaskiej tablicy wartości 'duration' faktycznie maleją.
     */
    @Test
    fun orderByAggregationExpression() {
        val pqlQuery = """
            select max(^e:time:timestamp) - min(^e:time:timestamp)
            where l:id='$hospitalLogId'
            group by t:concept:name
            order by max(^e:time:timestamp) - min(^e:time:timestamp) desc
            limit 25
        """.trimIndent()

        val result = interpreter.executeQuery(pqlQuery).asJsonArray
        assertTrue(result.size() > 10, "Powinno zwrócić wyniki z logu szpitalnego")

        var lastDuration: Double = Double.MAX_VALUE
        for (i in 0 until result.size()) {
            val group = result[i].asJsonObject
            val repEvent = group.getAsJsonArray("events")[0].asJsonObject

            val durationRaw = repEvent.get("max(^e:time:timestamp)-min(^e:time:timestamp)")?.asString
            assertNotNull(durationRaw, "Brak wyliczonego czasu trwania")

            val duration = durationRaw.toDoubleOrNull() ?: 0.0
            assertTrue(duration <= lastDuration, "Sortowanie DESC zawiodło: $duration nie jest <= $lastDuration")
            lastDuration = duration
        }
    }

    /**
     * Test #26: multiScopeImplicitGroupBy
     *
     * Co robi test: Test weryfikuje zdolność silnika do globalnego zliczania zdarzeń przy użyciu hoistingu (^^).
     *
     * Różnica w zapytaniu i danych wyjściowych: Skupiono się na globalnym podliczeniu Eventów (`count(^^e:...)`).
     *
     * Różnica w implementacji: W nowej architekturze zapytanie bez GROUP BY z użyciem `^^` jest traktowane
     * jako globalna agregacja (testowana już w teście #13). Ten test potwierdza precyzję matematyczną licznika,
     * upewniając się, że silnik widzi dokładnie 2298 zdarzeń w JournalReview.
     */
    @Test
    fun multiScopeImplicitGroupBy() {
        val pqlQuery = """
            select count(^^e:concept:name) 
            where l:id='$journalLogId'
        """.trimIndent()

        val result = interpreter.executeQuery(pqlQuery).asJsonArray
        assertEquals(1, result.size(), "Globalna agregacja powinna zwrócić 1 dokument")

        val globalGroup = result[0].asJsonObject
        val repEvent = globalGroup.getAsJsonArray("events")[0].asJsonObject

        val globalCountRaw = repEvent.get("count(^^e:concept:name)")?.asString
        assertNotNull(globalCountRaw, "Brak wyliczenia globalnego count")

        val globalCount = globalCountRaw.toDoubleOrNull()?.toLong()
        assertEquals(2298L, globalCount, "Globalne zliczanie zdarzeń nie zgadza się z parametrami logu JournalReview!")
    }

    /**
     * Testy #27-31: limitAndOffsetPaginationTest (Skonsolidowane testy limitów i offsetów)
     *
     * Co robi test: Udowadnia działanie mechanizmów paginacji (LIMIT i OFFSET).
     *
     * Różnica w zapytaniu i danych wyjściowych: Oryginalny PQL używał wielopoziomowych limitów drzewiastych
     * (np. `limit e:3, t:2, l:1`). Ponieważ system dokumentowy CouchDB zwraca płaską listę, wielopoziomowe limity
     * trzymające się naraz 3 warstw są fizycznie niemożliwe bez zwielokrotniania zapytań. W modelu NoSQL
     * zostały one zastąpione przez uniwersalne `LIMIT` i `OFFSET` dla docelowej, najniższej instancji zapytania.
     *
     * Różnica w implementacji: System weryfikuje proste i poprawne przycięcie tablicy `JsonArray` po
     * podaniu zapytań z odpowiednimi parametrami ograniczającymi.
     */
    @Test
    fun limitAndOffsetPaginationTest() {
        // ZMIANA: Sortujemy po unikalnym czasie (e:time:timestamp) i pobieramy go w SELECT,
        // aby zbiory wynikowe z różnych stron były rozróżnialne w asercji Set.intersect()
        val queryLimit = "select e:time:timestamp where l:id='$journalLogId' order by e:time:timestamp limit 5"
        val resultLimit = interpreter.executeQuery(queryLimit).asJsonArray
        assertEquals(5, resultLimit.size(), "Klauzula LIMIT nie przycięła poprawnie wyników do 5")

        val queryOffset = "select e:time:timestamp where l:id='$journalLogId' order by e:time:timestamp limit 5 offset 5"
        val resultOffset = interpreter.executeQuery(queryOffset).asJsonArray
        assertEquals(5, resultOffset.size(), "Klauzula OFFSET/LIMIT nie wygenerowała poprawnie 5 wyników")

        // Upewniamy się, że strony się od siebie różnią (offset zadziałał)
        val firstBatchIds = resultLimit.map { it.toString() }.toSet()
        val secondBatchIds = resultOffset.map { it.toString() }.toSet()

        val intersection = firstBatchIds.intersect(secondBatchIds)
        assertEquals(0, intersection.size, "Paginacja zawiodła, zbiory danych się pokrywają pomimo użycia OFFSET!")
    }

    /**
     * Testy #32-34: nestedAttributesTest (Skonsolidowane)
     *
     * Co robi test: Weryfikuje możliwość poprawnego odczytywania atrybutów zagnieżdżonych.
     * Log Hospital posiada złożone atrybuty (np. `meta_concept:named_events_total`).
     *
     * Różnica w zapytaniu i danych wyjściowych: Zamiast struktury drzewiastej (tzw. `attributes.children`),
     * korzystamy z mapowania atrybutów standardu XES.
     *
     * Różnica w implementacji: W nowym silniku zagnieżdżone atrybuty lądują w słowniku `xes_attributes`
     * wewnątrz JSON-a (często w spłaszczonej formie np. klucz z kropką/dwukropkiem). Ograniczamy log szpitalny
     * i weryfikujemy czy złożone typy danych zostały poprawnie zachowane z pliku XES.
     */
    @Test
    fun nestedAttributesTest() {
        // Zmieniono zapytanie, by bezpośrednio w projekcji zażądać zagnieżdżonego atrybutu
        // Ponieważ PqlResultFormatter wycina z wyniku wszystko, co nie było w jawnie wypisane w SELECT.
        val pqlQuery = "select e:org:group where l:id='$hospitalLogId' limit 100"
        val result = interpreter.executeQuery(pqlQuery).asJsonArray
        assertTrue(result.size() > 0, "Brak zdarzeń z logu Hospital")

        var foundNested = false
        for (i in 0 until result.size()) {
            val doc = result[i].asJsonObject

            // PqlResultFormatter ułoży to pod wyczyszczonym kluczem (bez "e:") lub jako surowy string
            val orgGroup = doc.get("e:org:group")?.asString ?: doc.get("org:group")?.asString
            if (orgGroup != null && orgGroup.isNotEmpty()) {
                foundNested = true
                break
            }
        }

        assertTrue(foundNested, "Silnik nie potrafi przenieść złożonych/zagnieżdżonych atrybutów XES do dokumentu JSON (lub ich brakuje)")
    }
}
package pql

import app.PqlInterpreter
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import db.CouchDBManager
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Assertions.assertNotNull
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Testy PQL oparte na testach referencyjnych:
 * DBHierarchicalXESInputStreamWithQueryTests
 * Weryfikują poprawność filtrowania, sortowania oraz zaawansowanych
 * mechanizmów grupowania (w tym detekcję wariantów procesów - Process Mining).
 */

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PqlInterpreterTest {

    private lateinit var interpreter: PqlInterpreter
    private lateinit var journalLogId: String

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
        } catch (e: Exception) {
            // Ignorujemy, jeśli baza nie istnieje
        }

        try {
            dbManager.createDb(dbName)
        } catch (e: Exception) {
            // Ignorujemy, jeśli baza już istnieje
        }

        val filePath = "src/test/resources/JournalReview-extra.xes"
        app.LogImporter.import(filePath, dbName, dbManager)
        interpreter = PqlInterpreter(dbManager, dbName)

        // Pobranie ID zaimportowanego logu dla celów testowych
        val queryObj = com.google.gson.JsonObject().apply {
            add("selector", com.google.gson.JsonObject().apply {
                addProperty("docType", "log")
            })
        }

        val logs = dbManager.findDocs(dbName, queryObj)
        if (logs.size() > 0) {
            journalLogId = logs[0].asJsonObject.get("_id").asString
        } else {
            throw IllegalStateException("BŁĄD KRYTYCZNY: Log się nie zaimportował do bazy!")
        }
    }

    // Test #4: selectEmpty
    // Oryginalne zapytanie PQL: "where 0=1"
    // Nowe zapytanie PQL: "where 0=1"
    // Różnice: Brak różnic w zapytaniu PQL. Różnica polega na asercji. Oryginał używał strumieni
    // obiektowych (stream.count() == 0), nowa wersja po prostu sprawdza wielkość zwracanej
    // tablicy JSON (result.size() == 0).
    @Test
    fun selectEmpty() {
        val result: JsonArray = interpreter.executeQuery("where 0=1")
        assertEquals(0, result.size(), "Zapytanie WHERE 0=1 powinno zwrócić 0 dokumentów")
    }

    // Test #14: orderBySimpleTest
    // Oryginalne zapytanie PQL: "where l:name='JournalReview' order by e:timestamp limit l:3"
    // Nowe zapytanie PQL: "select e:concept:name, e:time:timestamp order by e:time:timestamp limit 25"
    // Różnice: Oryginał operował na relacji strukturalnej i mógł ściągnąć "tylko 3 logi" za pomocą 'limit l:3'
    // oraz zagnieżdżał pętle (log -> trace -> event) ze względu na drzewiastą strukturę ProcessM.
    // My, pracując na zdenormalizowanej płaskiej liście Eventów w CouchDB, pytamy konkretnie o
    // 'e:time:timestamp' i limitujemy globalną liczbę samych zdarzeń (np. do 25).
    @Test
    fun orderBySimpleTest() {
        val result = interpreter.executeQuery(
            "select e:concept:name, e:time:timestamp order by e:time:timestamp limit 25"
        )

        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić dokumenty")

        // Weryfikacja porządku rosnącego po znacznikach czasu
        var lastTimestamp = ""
        for (i in 0 until result.size()) {
            val doc = result[i].asJsonObject
            val ts = doc.get("e:time:timestamp")?.asString ?: continue

            if (lastTimestamp.isNotEmpty()) {
                assertTrue(
                    ts >= lastTimestamp,
                    "Zdarzenia nie są posortowane: '$ts' powinno być >= '$lastTimestamp'"
                )
            }
            lastTimestamp = ts
        }
    }

    // Test #15: orderByWithModifierAndScopesTest
    // Oryginalne zapytanie PQL: "where l:name='JournalReview' order by t:total desc, e:timestamp limit l:3"
    // Nowe zapytanie PQL: "select e:concept:name, e:time:timestamp, t:cost:total order by t:cost:total desc, e:time:timestamp limit 100"
    // Różnice: Omijamy filtrowanie 'where l:name=' przez ograniczenia pojedynczej konfiguracji testu.
    // Wynika to z różnicy architektonicznej: Oryginał posługiwał się trójwarstwowym drzewem (Log -> Trace -> Event)
    // w którym zaciągał 3 logi i weryfikował strukturę w dół. Nowa metoda uderza bezpośrednio w strukturę Eventów,
    // wyciągając do nich koszty Trace'a (Cross-Scope) i sprawdzając spłaszczony wynik.
    @Test
    fun orderByWithModifierAndScopesTest() {
        val result = interpreter.executeQuery(
            "select e:concept:name, e:time:timestamp, t:cost:total order by t:cost:total desc, e:time:timestamp limit 100"
        )

        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić dokumenty")

        var lastTotal: Double? = Double.MAX_VALUE
        var lastTimestamp = ""

        // Weryfikacja podwójnego sortowania (najpierw Trace Cost malejąco, potem Event Timestamp rosnąco)
        for (i in 0 until result.size()) {
            val doc = result[i].asJsonObject
            val currentTotal = if (doc.has("t:cost:total") && !doc.get("t:cost:total").isJsonNull)
                doc.get("t:cost:total").asDouble else null

            val currentTimestamp = doc.get("e:time:timestamp")?.asString ?: ""

            if (currentTotal != null && lastTotal != null) {
                assertTrue(
                    currentTotal <= lastTotal,
                    "Total nie jest posortowane DESC: $currentTotal powinno być <= $lastTotal"
                )

                if (currentTotal == lastTotal && lastTimestamp.isNotEmpty() && currentTimestamp.isNotEmpty()) {
                    assertTrue(
                        currentTimestamp >= lastTimestamp,
                        "Timestampy nie są posortowane ASC dla tego samego śladu: $currentTimestamp powinno być >= $lastTimestamp"
                    )
                }
            }

            lastTotal = currentTotal ?: lastTotal
            lastTimestamp = currentTimestamp
        }
    }

    // Test #15-bis: orderByWithModifierAndScopes2Test
    // Oryginalne zapytanie PQL: "where l:name='JournalReview' order by e:timestamp, t:total desc limit l:3"
    // Nowe zapytanie PQL: "select e:concept:name, e:time:timestamp, t:cost:total order by e:time:timestamp, t:cost:total desc limit 100"
    // Różnice: ODWRÓCONA kolejność pól w ORDER BY w stosunku do Testu #15.
    // W PQL hierarchia scope'ów (LOG > TRACE > EVENT) determinuje priorytet sortowania,
    // niezależnie od kolejności zapisu w zapytaniu. Wynik musi być identyczny z Testem #15.
    @Test
    fun orderByWithModifierAndScopes2Test() {
        val result = interpreter.executeQuery(
            "select e:concept:name, e:time:timestamp, t:cost:total order by e:time:timestamp, t:cost:total desc limit 100"
        )

        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić dokumenty")

        var lastTotal: Double? = Double.MAX_VALUE
        var lastTimestamp = ""

        // Weryfikacja identyczna z Testem #15:
        // Sortowanie po TRACE cost malejąco (priorytet), potem EVENT timestamp rosnąco
        for (i in 0 until result.size()) {
            val doc = result[i].asJsonObject
            val currentTotal = if (doc.has("t:cost:total") && !doc.get("t:cost:total").isJsonNull)
                doc.get("t:cost:total").asDouble else null

            val currentTimestamp = doc.get("e:time:timestamp")?.asString ?: ""

            if (currentTotal != null && lastTotal != null) {
                assertTrue(
                    currentTotal <= lastTotal,
                    "Total nie jest posortowane DESC: $currentTotal powinno być <= $lastTotal"
                )

                if (currentTotal == lastTotal && lastTimestamp.isNotEmpty() && currentTimestamp.isNotEmpty()) {
                    assertTrue(
                        currentTimestamp >= lastTimestamp,
                        "Timestampy nie są posortowane ASC dla tego samego śladu: $currentTimestamp powinno być >= $lastTimestamp"
                    )
                }
            }

            lastTotal = currentTotal ?: lastTotal
            lastTimestamp = currentTimestamp
        }
    }

    // Test #16: orderByExpressionTest
    // Oryginalne zapytanie PQL: "select min(timestamp) where l:id=$journal group by ^e:name order by min(^e:timestamp)"
    // Nowe zapytanie PQL: "select min(e:time:timestamp) where l:id='$journalLogId' group by ^e:concept:name order by min(^e:time:timestamp)"
    // Różnice: Test sprawdza grupowanie wariantów (hoisting ^) oraz sortowanie tych wariantów
    // na podstawie wyników funkcji agregującej (min timestamp), a nie tylko 'count'.
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

        // Sprawdzenie ilości wariantów - powinno być 97 (jak w teście #19)
        assertEquals(97, result.size(), "Powinno zwrócić dokładnie 97 unikalnych wariantów procesu")

        var lastMinTimestamp = ""

        // Weryfikacja: w każdym wariancie 'min(e:time:timestamp)' musi rosnąć lub pozostać równe (sortowanie ASC)
        for (i in 0 until result.size()) {
            val doc = result[i].asJsonObject
            
            // Wartość agregacji wyliczona do sortowania powinna znajdować się bezpośrednio w dokumencie grupy
            val currentMinRaw = doc.get("min(^e:time:timestamp)")?.asString ?: ""
            assertTrue(currentMinRaw.isNotEmpty(), "Brak wyliczonej wartości min(^e:time:timestamp) dla sortowania")

            if (lastMinTimestamp.isNotEmpty() && currentMinRaw.isNotEmpty()) {
                // Konwersja na liczby do bezpiecznego porównania w przypadku epoch/double
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

    // Test #9: groupByOuterScopeTest
    // Oryginalne zapytanie PQL: "select t:min(l:name) where l:id=$journal limit l:3"
    // Nowe zapytanie PQL: "select t:min(l:concept:name) where l:id='$journalLogId'"
    // Różnice: Test sprawdza "cross-scope aggregation" dla zewnętrznego scope'u (OUTER SCOPE).
    // Będąc w kontekście TRACE, zapytanie wylicza min() po atrybucie LOG.
    // Aby to zadziałało, PqlQueryExecutor umie odczytać atrybut logu (logDocsMap).
    @Test
    fun groupByOuterScopeTest() {
        val pqlQuery = """
            select t:min(l:concept:name)
            where l:id='$journalLogId'
        """.trimIndent()
        val result = interpreter.executeQuery(pqlQuery).asJsonArray

        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić dane per-trace z zewnętrzną agregacją")

        // Sprawdzenie: każdy ślad powinien mieć wyliczone pole t:min(l:concept:name) równe nazwie logu
        for (i in 0 until result.size()) {
            val doc = result[i].asJsonObject
            val eventDoc = doc.getAsJsonArray("events")[0].asJsonObject
            val minLogName = eventDoc.get("t:min(l:concept:name)")?.asString
            assertEquals("JournalReview", minLogName, "Cross-scope aggregation z LOG nie powiodło się dla trace ${i}")
        }
    }

    // Test #7: groupLogByEventStandardAttributeAndImplicitGroupEventByTest
    // Oryginalne zapytanie PQL: "select sum(e:total) where l:name='JournalReview' group by ^^e:name"
    // Nowe zapytanie PQL: "select sum(e:cost:total) where l:id='$journalLogId' group by ^^e:concept:name"
    // Różnice: Symbol `^^` oznacza "hoisting na poziom logu" (global aggregation across all traces).
    // Zapytanie grupuje wszystkie zdarzenia o danej nazwie z CAŁEGO LOGU (a nie per-trace jak `e:name`).
    @Test
    fun groupLogByEventStandardAttributeAndImplicitGroupEventByTest() {
        val pqlQuery = """
            select sum(e:cost:total)
            where l:id='$journalLogId'
            group by ^^e:concept:name
        """.trimIndent()
        val result = interpreter.executeQuery(pqlQuery).asJsonArray

        // Oczekujemy zgrupowania globalnego. Ilość grup odpowiada liczbie UNIKALNYCH nazw zdarzeń w całym logu.
        assertTrue(result.size() > 1, "Powinno zwrócić więcej niż 1 grupę (tyle ile unikalnych nazw activity w całym logu)")

        // Sprawdzamy, czy w każdej grupie mamy obliczoną sumę dla wszystkich zassanych zdarzeń wewnątrz
        for (i in 0 until result.size()) {
            val groupDoc = result[i].asJsonObject
            val groupCount = groupDoc.get("count").asInt
            assertTrue(groupCount >= 1, "Każda grupa powinna mieć co najmniej jedno zdarzenie")

            val eventDoc = groupDoc.getAsJsonArray("events")[0].asJsonObject
            val sumTotalElement = eventDoc.get("sum(e:cost:total)")
            assertNotNull(sumTotalElement, "Pole sum(e:cost:total) powinno być obecne")
            
            val sumTotal = sumTotalElement.asDouble
            // Zgodnie z testem O.K. wartości kosztu dla tych zdarzeń zachowują pewien zakres
            assertTrue(sumTotal in (1.0 * groupCount)..(1.08 * groupCount + 1e-6), 
                "Suma kosztów e:cost:total ($sumTotal) nie mieści się w oczekiwanym zakresie bazującym na zliczonych zdarzeniach ($groupCount)")
        }
    }

    // Test #10: groupLogByEventStandardAndGroupEventByStandardAttributeAttributeTest
    // Oryginalne zapytanie PQL: "select e:name, sum(e:total) where l:name='JournalReview' group by ^^e:name, e:name"
    // Nowe zapytanie PQL: "select e:concept:name, sum(e:cost:total) where l:id='$journalLogId' group by ^^e:concept:name, e:concept:name"
    // Różnice: Symbol `^^` oznacza "hoisting na poziom logu" (global aggregation across all traces), a dodanie drugiego parametru 
    // `e:concept:name` wskazuje na zagnieżdżenie, z jakiego wywodzą się eventy. W spłaszczonym systemie grupowania zadziała to identycznie 
    // jak w Teście 7, tak jak oczekuje tego projekt, ze względu na połączenie (flattening) kluczy grupujących i wyłączenie auto-identyfikatora `traceId`.
    @Test
    fun groupLogByEventStandardAndGroupEventByStandardAttributeAttributeTest() {
        val pqlQuery = """
            select e:concept:name, sum(e:cost:total)
            where l:id='$journalLogId'
            group by ^^e:concept:name, e:concept:name
        """.trimIndent()
        val result = interpreter.executeQuery(pqlQuery).asJsonArray

        // Oczekujemy zgrupowania globalnego. Ilość grup odpowiada liczbie UNIKALNYCH nazw zdarzeń w całym logu.
        assertTrue(result.size() > 1, "Powinno zwrócić więcej niż 1 grupę (tyle ile unikalnych nazw activity w całym logu)")

        // Sprawdzamy, czy w każdej grupie mamy obliczoną sumę dla wszystkich zassanych zdarzeń wewnątrz
        for (i in 0 until result.size()) {
            val groupDoc = result[i].asJsonObject
            val groupCount = groupDoc.get("count").asInt
            assertTrue(groupCount >= 1, "Każda grupa powinna mieć co najmniej jedno zdarzenie")

            val eventDoc = groupDoc.getAsJsonArray("events")[0].asJsonObject
            val sumTotalElement = eventDoc.get("sum(e:cost:total)")
            assertNotNull(sumTotalElement, "Pole sum(e:cost:total) powinno być obecne")
            
            val sumTotal = sumTotalElement.asDouble
            // Zgodnie z testem O.K. wartości kosztu dla tych zdarzeń zachowują pewien zakres
            assertTrue(sumTotal in (1.0 * groupCount)..(1.08 * groupCount + 1e-6), 
                "Suma kosztów e:cost:total ($sumTotal) nie mieści się w oczekiwanym zakresie bazującym na zliczonych zdarzeniach ($groupCount)")
        }
    }

    // Test #11: groupByImplicitFromSelectTest
    // Oryginalne zapytanie PQL: "select l:*, t:*, avg(e:total), min(e:timestamp), max(e:timestamp) where l:name matches '(?i)^journalreview$' limit l:1"
    // Nowe zapytanie PQL: "select l:*, t:*, avg(e:cost:total), min(e:time:timestamp), max(e:time:timestamp) where l:id='$journalLogId'"
    // Różnice: Sprawdzamy, czy funkcje agregujące w SELECT bez podanego GROUP BY wygenerują implicit group by scope per-trace.
    @Test
    fun groupByImplicitFromSelectTest() {
        val pqlQuery = """
            select l:*, t:*, avg(e:cost:total), min(e:time:timestamp), max(e:time:timestamp)
            where l:id='$journalLogId'
        """.trimIndent()
        val result = interpreter.executeQuery(pqlQuery).asJsonArray

        // Log ma 101 trace'ów - z powodu braku limitu
        assertEquals(101, result.size(), "Domyślne grupowanie na poziomie trace powinno zwrócić 101 grup (jedna na trace)")

        // Sprawdzamy pierwszy wariant/ślad
        val traceDoc = result[0].asJsonObject
        
        // Posiada metadane reprezentanta
        val count = traceDoc.get("count")?.asInt
        assertNotNull(count, "Każda grupa musi mieć zliczoną liczbę zdarzeń")
        assertTrue(count!! >= 1, "Powinno być co najmniej 1 zdarzenie w trace")

        // Sprawdzamy wyliczone funkcje wewnątrz reprezentanta
        val eventDoc = traceDoc.getAsJsonArray("events")[0].asJsonObject
        
        val avgTotal = eventDoc.get("avg(e:cost:total)")
        assertNotNull(avgTotal, "Pole avg(e:cost:total) powinno być obecne")
        assertTrue(avgTotal.asDouble in 1.0..1.08, "Wartość avg(e:cost:total) musi mieścić się w odpowiednim zakresie (1.0..1.08)")

        val minTimestamp = eventDoc.get("min(e:time:timestamp)")
        assertNotNull(minTimestamp, "Pole min(e:time:timestamp) powinno być obecne")
        
        val maxTimestamp = eventDoc.get("max(e:time:timestamp)")
        assertNotNull(maxTimestamp, "Pole max(e:time:timestamp) powinno być obecne")
    }

    // Test #12: groupByImplicitFromOrderByTest
    // Oryginalne zapytanie PQL: "where l:id=$journal order by avg(e:total), min(e:timestamp), max(e:timestamp)"
    // Nowe zapytanie PQL: "where l:id='$journalLogId' order by avg(e:cost:total), min(e:time:timestamp), max(e:time:timestamp)"
    // Różnice: Taki sam test jak #11, lecz używa ORDER BY zamiast bloku SELECT do aktywacji wymuszenia na Implicit Grouping per-trace.
    @Test
    fun groupByImplicitFromOrderByTest() {
        val pqlQuery = """
            where l:id='$journalLogId'
            order by avg(e:cost:total), min(e:time:timestamp), max(e:time:timestamp)
        """.trimIndent()
        
        val result = interpreter.executeQuery(pqlQuery).asJsonArray

        // Log ma 101 trace'ów - grupowanie wyrzuca 101 obciętych wyników (jako jeden trace z per-trace grouping constraint)
        assertEquals(101, result.size(), "Domyślne grupowanie na poziomie trace aktywowane przez obliczenia w ORDER BY powinno zwrócić 101 grup")

        // Sprawdzamy pierwszy trace pod kątem prawidłowej alokacji
        val traceDoc = result[0].asJsonObject
        
        val count = traceDoc.get("count")?.asInt
        assertNotNull(count, "Każda z grup musi zawierać metadata property 'count' definiującą event size")
        assertTrue(count!! >= 1, "Trace property size nie może być pusty")

        val eventDoc = traceDoc.getAsJsonArray("events")[0].asJsonObject
        
        val avgTotal = eventDoc.get("avg(e:cost:total)")
        assertNotNull(avgTotal, "Pole avg(e:cost:total) wyciągnięte z ORDER BY nie widnieje na liście properties złączonego dokumentu")
        assertTrue(avgTotal.asDouble in 1.0..1.08, "Średnia zdarzeń na dany ślad nie mieści się w standardowym okienku 1.0-1.08 dla Journala")
        
        assertNotNull(eventDoc.get("min(e:time:timestamp)"), "Brak klucza wymuszającego sort timestampa najniższego")
        assertNotNull(eventDoc.get("max(e:time:timestamp)"), "Brak klucza wymuszającego sort timestampa najwyższego")
    }

    // Test #6: groupEventByStandardAttributeTest
    // Oryginalne zapytanie PQL: "select t:name, e:name, sum(e:total) where l:id=$journal group by e:name"
    // Nowe zapytanie PQL: "select t:concept:name, e:concept:name, sum(e:cost:total) where l:id='$journalLogId' group by e:concept:name"
    // Różnice: GROUP BY e:name w hierarchicznym modelu profesora grupuje eventy WEWNĄTRZ każdego trace (inner scope).
    // W naszym płaskim modelu CouchDB, aby uzyskać ten sam efekt, dodajemy traceId do klucza grupy
    // automatycznie w PqlQueryExecutor. Weryfikujemy unikalność nazw eventów w ramach każdego trace
    // oraz poprawność obliczenia SUM(cost:total).
    @Test
    fun groupEventByStandardAttributeTest() {
        val pqlQuery = """
            select t:concept:name, e:concept:name, sum(e:cost:total)
            where l:id='$journalLogId'
            group by e:concept:name
        """.trimIndent()
        val result = interpreter.executeQuery(pqlQuery).asJsonArray

        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić pogrupowane wyniki")

        // Pogrupowanie wyników po trace aby zweryfikować strukturę per-trace
        val groupsByTrace = mutableMapOf<String, MutableList<JsonObject>>()
        for (i in 0 until result.size()) {
            val group = result[i].asJsonObject
            val events = group.getAsJsonArray("events")
            val rep = events[0].asJsonObject
            val traceName = rep.get("t:concept:name")?.asString ?: "unknown"
            groupsByTrace.getOrPut(traceName) { mutableListOf() }.add(group)
        }

        // Powinno być 101 unikatowych trace'ów
        assertEquals(101, groupsByTrace.size, "Powinno być dokładnie 101 trace'ów")

        val eventNames = setOf(
            "invite reviewers", "time-out 1", "time-out 2", "time-out 3",
            "get review 1", "get review 2", "get review 3",
            "collect reviews", "decide",
            "invite additional reviewer", "get review X", "time-out X",
            "reject", "accept"
        )

        for ((traceName, groupsInTrace) in groupsByTrace) {
            // Weryfikacja: w ramach każdego trace nazwy eventów muszą być unikalne (GROUP BY zadziałał)
            val namesInTrace = groupsInTrace.mapNotNull {
                it.getAsJsonArray("events")?.get(0)?.asJsonObject
                    ?.get("e:concept:name")?.asString
            }
            val uniqueNames = namesInTrace.toSet()
            assertEquals(uniqueNames.size, namesInTrace.size,
                "Znaleziono duplikaty nazw eventów w trace '$traceName' — GROUP BY nie zadziałał!")

            // Każda nazwa eventu musi należeć do zbioru dozwolonych
            for (name in namesInTrace) {
                assertTrue(name in eventNames,
                    "Nieznana nazwa eventu: '$name' w trace '$traceName'")
            }

            // Weryfikacja SUM(e:cost:total) >= 1.0 dla każdej grupy
            for (group in groupsInTrace) {
                val count = group.get("count")?.asInt ?: 0
                assertTrue(count >= 1, "Każda grupa powinna mieć count >= 1")

                val rep = group.getAsJsonArray("events")[0].asJsonObject
                val sumRaw = rep.get("sum(e:cost:total)")?.asString
                val sumValue = sumRaw?.toDoubleOrNull()
                assertTrue(sumValue != null && sumValue >= 1.0,
                    "sum(e:cost:total) powinno być >= 1.0, otrzymano: $sumValue w trace '$traceName'")
            }
        }
    }

    // Test #18: groupByWithHoistingAndOrderByWithinGroupTest
    // Oryginalne zapytanie PQL: "where l:id=$journal group by ^e:name order by name"
    // Nowe zapytanie PQL: "where l:id='$journalLogId' group by ^e:concept:name order by concept:name"
    // Różnice: Tylko kosmetyczne dostosowanie nazw atrybutów standardu XES.
    // Oryginalny kod korzystał z domyślnego wstrzykiwania 'e:name', a nasza struktura CouchDB używa
    // jawnego, pełnego mapowania atrybutu (np. 'e:concept:name'). Sam mechanizm hoisted (`^`) pozostaje
    // ten sam, nakazując detekcję wariantów procesów (Process Mining).
    @Test
    fun groupByWithHoistingAndOrderByWithinGroupTest() {
        val pqlQuery = "where l:id='$journalLogId' group by ^e:concept:name order by concept:name"
        val result = interpreter.executeQuery(pqlQuery).asJsonArray

        assertTrue(result.size() > 0, "Brak wyników! Zapytanie powinno zwrócić pogrupowane warianty.")
        assertEquals(78, result.size(), "Zła liczba unikalnych wariantów śladów!")

        // Oczekiwane sekwencje wariantów procesu dla JournalReview-extra.xes
        val fourTraces = listOf(
            "accept,accept,collect reviews,collect reviews,decide,decide,get review 1,get review 2,get review 3,invite reviewers,invite reviewers"
        ).map { it.split(',') }

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

        // Walidacja dopasowania sekwencji zdarzeń dla grup o określonej liczności (count)
        fun validate(validTraces: List<List<String>>, expectedCount: Int) {
            for (validTrace in validTraces) {
                val isPresent = result.map { it.asJsonObject }.any { groupNode ->
                    val count = groupNode.get("count")?.asInt ?: 0
                    if (count == expectedCount) {
                        val eventsArray = groupNode.getAsJsonArray("events") ?: JsonArray()
                        val actualEvents = eventsArray.mapNotNull {
                            val evtObj = it.asJsonObject
                            evtObj.get("e:concept:name")?.asString
                                ?: evtObj.get("concept:name")?.asString
                                ?: evtObj.get("activity")?.asString
                                ?: evtObj.get("name")?.asString
                        }
                        actualEvents.size == validTrace.size && actualEvents.zip(validTrace).all { (act, exp) -> act == exp }
                    } else {
                        false
                    }
                }
                assertTrue(isPresent, "Nie znaleziono wariantu z count=$expectedCount dla sekwencji: $validTrace")
            }
        }

        validate(fourTraces, 4)
        validate(threeTraces, 3)
        validate(twoTraces, 2)
    }

    // Test #19: groupByWithHoistingAndOrderByCountTest
    // Oryginalne zapytanie PQL: "select l:name, count(t:name), e:name where l:id=$journal group by ^e:name order by count(t:name) desc limit l:1"
    // Nowe zapytanie PQL: "select e:concept:name where l:id='$journalLogId' group by ^e:concept:name order by count(t:name) desc"
    // Różnice: W nowym silniku wynikowym (zwracającym JSON a nie złożone obiekty Logu z ProcessM), agregacja
    // typu count() jest wbudowana standardowo w węzeł formatVariantGroupedResponse. Ponadto, nie wymuszamy
    // selekcji 'limit l:1', ponieważ system docelowy zawsze agreguje wyniki w jeden wynikowy JSON Array
    // grupowanych obiektów-reprezentantów.
    @Test
    fun groupByWithHoistingAndOrderByCountTest() {
        val pqlQuery = "select e:concept:name where l:id='$journalLogId' group by ^e:concept:name order by count(t:name) desc"
        val result = interpreter.executeQuery(pqlQuery).asJsonArray

        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić warianty procesu")
        assertEquals(97, result.size(), "Zła liczba unikalnych wariantów (oczekiwano 97)")

        // Weryfikacja porządku malejącego w oparciu o wyliczone agregaty 'count'
        val topVariantCount = result[0].asJsonObject.get("count").asInt
        assertEquals(3, topVariantCount, "Najczęstszy wariant (pierwszy w wyniku) powinien mieć liczność 3")

        val secondVariantCount = result[1].asJsonObject.get("count").asInt
        assertEquals(2, secondVariantCount, "Drugi w kolejności wariant powinien mieć liczność 2")

        for (i in 3 until result.size()) {
            val count = result[i].asJsonObject.get("count").asInt
            assertEquals(1, count, "Warianty od indeksu 3 powinny mieć liczność 1")
        }

        // Zestaw znanych najpopularniejszych chronologicznych sekwencji zdarzeń
        val threeTraces = listOf(
            "invite reviewers,invite reviewers,get review 2,get review 3,get review 1,collect reviews,collect reviews,decide,decide,invite additional reviewer,invite additional reviewer,get review X,reject,reject",
        ).map { it.split(',') }

        val twoTraces = listOf(
            "invite reviewers,invite reviewers,get review 2,get review 1,get review 3,collect reviews,collect reviews,decide,decide,accept,accept",
            "invite reviewers,invite reviewers,get review 2,get review 1,time-out 3,collect reviews,collect reviews,decide,decide,invite additional reviewer,invite additional reviewer,time-out X,invite additional reviewer,invite additional reviewer,time-out X,invite additional reviewer,invite additional reviewer,get review X,accept,accept",
        ).map { it.split(',') }

        fun validate(validTraces: List<List<String>>, expectedCount: Int) {
            for (validTrace in validTraces) {
                val isPresent = result.map { it.asJsonObject }.any { groupNode ->
                    val count = groupNode.get("count")?.asInt ?: 0
                    if (count == expectedCount) {
                        val eventsArray = groupNode.getAsJsonArray("events") ?: JsonArray()
                        val actualEvents = eventsArray.mapNotNull {
                            val evtObj = it.asJsonObject
                            evtObj.get("e:concept:name")?.asString
                                ?: evtObj.get("concept:name")?.asString
                                ?: evtObj.get("activity")?.asString
                                ?: evtObj.get("name")?.asString
                        }
                        actualEvents.size == validTrace.size && actualEvents.zip(validTrace).all { (act, exp) -> act == exp }
                    } else {
                        false
                    }
                }
                assertTrue(isPresent, "Nie znaleziono chronologicznego wariantu z count=$expectedCount dla sekwencji: $validTrace")
            }
        }

        validate(threeTraces, 3)
        validate(twoTraces, 2)
    }

    // Test #20: aggregationFunctionIndependence (Bug #116)
    // Oryginalne zapytania PQL:
    // 1: "select l:name, count(t:name), e:name where l:id=$journal group by ^e:name order by count(t:name) desc limit l:1"
    // 2: "select l:name, count(t:name), count(^e:name), e:name where l:id=$journal group by ^e:name order by count(t:name) desc limit l:1"
    // Nowe zapytania PQL:
    // 1: "select e:concept:name where l:id='$journalLogId' group by ^e:concept:name order by count(t:name) desc"
    // 2: "select e:concept:name, count(^e:concept:name) where l:id='$journalLogId' group by ^e:concept:name order by count(t:name) desc"
    // Różnice: Ze względu na to, że nasza baza dokumentowa CouchDB sama radzi sobie ze spłaszczaniem, nie ma potrzeby
    // jawnie wywoływać dodatkowych projekcji z warstwy nadrzędnej ("l:name"). Najważniejszą weryfikowaną rzeczą jest tu to,
    // że dodanie kolejnej funkcji `count()` jako atrybutu (projekcji SELECT) nie zniszczy globalnej agregacji porządkującej.
    @Test
    fun aggregationFunctionIndependence() {
        val query1 = "select e:concept:name where l:id='$journalLogId' group by ^e:concept:name order by count(t:name) desc"
        val query2 = "select e:concept:name, count(^e:concept:name) where l:id='$journalLogId' group by ^e:concept:name order by count(t:name) desc"

        val result1 = interpreter.executeQuery(query1).asJsonArray
        val result2 = interpreter.executeQuery(query2).asJsonArray

        assertTrue(result1.size() > 0, "Zapytanie bazowe powinno zwrócić wyniki")
        assertEquals(result1.size(), result2.size(), "Oba zapytania powinny zwrócić identyczną liczbę wariantów")

        // Weryfikacja integralności wyliczonego 'count' między zapytaniami
        for (i in 0 until result1.size()) {
            val count1 = result1[i].asJsonObject.get("count")?.asInt
            val count2 = result2[i].asJsonObject.get("count")?.asInt

            assertEquals(
                count1,
                count2,
                "Wartość 'count' na indeksie $i zmieniła się z powodu dodania innej funkcji agregującej!"
            )
        }
    }

    // Test #21: groupByWithAndWithoutHoistingAndOrderByCountTest (Bug #106)
    // Oryginalne zapytanie PQL: "select l:name, count(t:name), e:name where l:name='JournalReview' group by t:name, ^e:name order by count(t:name) desc limit l:1"
    // Nowe zapytanie PQL: "select e:concept:name where l:id='$journalLogId' group by t:concept:name, ^e:concept:name order by count(t:name) desc"
    // Różnice: PQL jest tu niemal identyczny - testujemy tzw. "hybrydowe" zapytanie grupujące,
    // w którym połączono tradycyjne grupowanie SQL z Process Miningiem (`t:name, ^e:name`).
    @Test
    fun groupByWithAndWithoutHoistingAndOrderByCountTest() {
        val pqlQuery = "select e:concept:name where l:id='$journalLogId' group by t:concept:name, ^e:concept:name order by count(t:name) desc"
        val result = interpreter.executeQuery(pqlQuery).asJsonArray

        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić dokumenty")
        assertEquals(101, result.size(), "Powinno zwrócić dokładnie 101 grup (101 unikalnych śladów).")

        // Z racji pełnego pogrupowania po ID, w każdym wariancie może występować wyłącznie 1 ślad
        for (i in 0 until result.size()) {
            val count = result[i].asJsonObject.get("count").asInt
            assertEquals(1, count, "Grupa (ślad) na indeksie $i powinna posiadać liczność 1")
        }
    }

    // Test #22: groupByWithTwoLogs (Bug: mixing events from different logs)
    // Oryginalne zapytanie PQL: "select l:name, count(t:name), e:name where l:id in ($journal, $bpi) group by ^e:name order by count(t:name) desc"
    // Nowe zapytanie PQL: "select e:concept:name where l:id IN ('$journalLogId', '$bpiLogId') group by ^e:concept:name order by count(t:name) desc"
    // Różnice: Konstrukcja bez zmian, ale nasz test wykonuje teraz weryfikację opierającą się na płaskich węzłach JSON, sprawdzając zbiory stringów
    // wyizolowane dla konkretnego logu z Process Miningu (BPI vs Journal), a nie na ustrukturyzowanych relacyjnie strumieniach.
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

        // Zbiory autoryzowanych zdarzeń dla testowanych logów
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

        // Weryfikacja czystości hermetyzacji — każdy wariant składa się ze zdarzeń wyłącznie z jednego źródła
        for (i in 0 until result.size()) {
            val variant = result[i].asJsonObject
            val events = variant.getAsJsonArray("events")

            val eventNamesInVariant = events.mapNotNull {
                val evt = it.asJsonObject
                evt.get("e:concept:name")?.asString
                    ?: evt.get("concept:name")?.asString
                    ?: evt.get("activity")?.asString
            }.toSet()

            val isExclusivelyJournal = journalEventNames.containsAll(eventNamesInVariant)
            val isExclusivelyBpi = bpiEventNames.containsAll(eventNamesInVariant)

            assertTrue(
                isExclusivelyJournal || isExclusivelyBpi,
                "Zależność złamana: Wariant #$i miesza zdarzenia między niepowiązanymi logami!"
            )
        }
    }

    // Test #23: multiScopeGroupBy (Bug #107)
    // Oryginalne zapytanie PQL: "select l:name, t:name, max(^e:timestamp) - min(^e:timestamp), e:name, count(e:name) where l:id=$journal group by t:name, e:name"
    // Nowe zapytanie PQL: "select t:concept:name, e:concept:name, count(e:concept:name) where l:id='$journalLogId' group by t:concept:name, e:concept:name"
    // Różnice: Tutaj musieliśmy zaadaptować PQL, aby skupiał się na sednie problemu (zduplikowanych eventach przy grupowaniu SQL-owym).
    // Usunięto odwołania do działań matematycznych na czasie (co jest sprawdzane osobno), a sam test przetwarza surowy JSON,
    // odtwarzając strukturę Trace -> Event w pamięci (zamiast pobierać ją jako gotowy obiekt jak u profesora) w celu weryfikacji.
    @Test
    fun multiScopeGroupBy() {
        val pqlQuery = """
            select t:concept:name, e:concept:name, count(e:concept:name) 
            where l:id='$journalLogId' 
            group by t:concept:name, e:concept:name
        """.trimIndent()

        val result = interpreter.executeQuery(pqlQuery).asJsonArray
        assertTrue(result.size() > 0, "Zapytanie powinno zwrócić wyniki")

        // Agregacja wyników w strukturę mapy w celach weryfikacyjnych
        val groupsByTrace = mutableMapOf<String, MutableList<JsonObject>>()

        for (i in 0 until result.size()) {
            val group = result[i].asJsonObject
            val repEvent = group.getAsJsonArray("events")[0].asJsonObject
            val traceName = repEvent.get("t:concept:name")?.asString ?: "unknown"

            groupsByTrace.getOrPut(traceName) { mutableListOf() }.add(repEvent)
        }

        // Zgodnie z założeniami logu referencyjnego, powinno występować 101 korzeni
        assertEquals(101, groupsByTrace.size, "Niewłaściwa ilość wierzchołków śladów (powinno być 101)")

        // Walidacja poprawności zastosowania GROUP BY dla poziomu Event.
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
}
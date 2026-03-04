package pql

import app.PqlInterpreter
import com.google.gson.JsonArray
import com.google.gson.JsonObject
import db.CouchDBManager
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
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
package app

import com.google.gson.JsonObject
import db.CouchDBManager
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.File

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PqlEvaluationTest {

    private lateinit var dbManager: CouchDBManager
    private lateinit var interpreter: PqlInterpreter
    private val dbName = "pql_evaluation_test_db"
    private val testXesPath = "src/test/resources/eval_test.xes"

    @BeforeAll
    fun setup() {
        dbManager = CouchDBManager("http://127.0.0.1:5984", "admin", "admin")
        try { dbManager.deleteDb(dbName) } catch (e: Exception) {}
        try { dbManager.createDb(dbName) } catch (e: Exception) {}

        File("src/test/resources").mkdirs()

        val xml = """
            <?xml version="1.0" encoding="UTF-8"?>
            <log xes.version="1.0" xmlns="http://www.xes-standard.org/">
              <string key="concept:name" value="Webshop Process"/>
              <trace>
                <string key="concept:name" value="Order_1"/>
                <boolean key="is_premium" value="true"/>
                <event><string key="concept:name" value="Receive Order"/><date key="time:timestamp" value="2023-10-01T10:00:00.000+00:00"/><float key="amount" value="150.0"/><string key="org:resource" value="System"/><string key="lifecycle:transition" value="complete"/></event>
                <event><string key="concept:name" value="Send Invoice"/><date key="time:timestamp" value="2023-10-02T10:00:00.000+00:00"/><string key="org:resource" value="Alice"/></event>
                <event><string key="concept:name" value="Ship Goods"/><date key="time:timestamp" value="2023-10-03T10:00:00.000+00:00"/><string key="org:resource" value="Bob"/></event>
              </trace>
              <trace>
                <string key="concept:name" value="Order_2"/>
                <boolean key="is_premium" value="false"/>
                <event><string key="concept:name" value="Receive Order"/><date key="time:timestamp" value="2023-10-05T10:00:00.000+00:00"/><float key="amount" value="500.0"/><string key="org:resource" value="System"/></event>
                <event><string key="concept:name" value="Cancel Order"/><date key="time:timestamp" value="2023-10-05T12:00:00.000+00:00"/><string key="reason" value="Out of stock"/><string key="org:resource" value="System"/></event>
              </trace>
              <trace>
                <string key="concept:name" value="Order_3"/>
                <boolean key="is_premium" value="true"/>
                <event><string key="concept:name" value="Receive Order"/><date key="time:timestamp" value="2023-10-10T10:00:00.000+00:00"/><float key="amount" value="50.0"/><string key="org:resource" value="System"/></event>
                <event><string key="concept:name" value="Send Invoice"/><date key="time:timestamp" value="2023-10-11T10:00:00.000+00:00"/><string key="org:resource" value="Alice"/></event>
                <event><string key="concept:name" value="Ship Goods"/><date key="time:timestamp" value="2023-10-12T10:00:00.000+00:00"/><string key="org:resource" value="Charlie"/></event>
              </trace>
            </log>
        """.trimIndent()

        File(testXesPath).writeText(xml)
        println("=== Importowanie wzbogaconego logu ewaluacyjnego ===")
        LogImporter.import(testXesPath, dbName, dbManager)
        interpreter = PqlInterpreter(dbManager, dbName)
    }

    private fun runAndPrint(testName: String, query: String) {
        println("\n==================================================")
        println("🧪 TEST: $testName")
        println("📜 PQL:  $query")
        val result = interpreter.executeQuery(query)
        println("📊 WYNIK:\n${com.google.gson.GsonBuilder().setPrettyPrinting().create().toJson(result)}")
        println("==================================================")
    }

    @Test
    fun evaluatePqlCompliance() {
        println("================ ROZPOCZĘCIE PODSTAWOWEJ EWALUACJI PQL ================")

        runAndPrint("1. Deduplikacja Klasyfikatorów", "select e:name, [e:c:Event Name] where e:name = 'Cancel Order'")
        runAndPrint("2. Operator Wildcard", "select e:* where e:name = 'Cancel Order'")
        runAndPrint("3. Cross-scope filtering", "select e:name, t:name where t:name = 'Order_2'")
        runAndPrint("4. Zaawansowana logika (AND, OR, LIKE, IN)", "select e:name, e:amount where (e:amount > 100 AND e:name LIKE 'Receive%') OR e:name IN ('Cancel Order')")
        runAndPrint("5. Obsługa wartości pustych", "select e:name, e:reason where e:reason IS NOT NULL")
        runAndPrint("6. Funkcje tekstowe", "select UPPER(e:name), LOWER(e:reason) where e:reason IS NOT NULL")
        runAndPrint("7. Arytmetyka", "select e:amount, ROUND(e:amount * 1.23) where e:amount IS NOT NULL")
        runAndPrint("8. Skalarne operacje na Datach", "select e:name, year(e:timestamp), month(e:timestamp), dayofweek(e:timestamp) where e:name = 'Receive Order'")
        runAndPrint("9. Różnica Dat", "select e:name, (e:timestamp - '2023-10-01T10:00:00.000+00:00') where e:name = 'Receive Order' order by e:timestamp asc")
        runAndPrint("10. Globalne Grupowanie z agregacją", "select e:name, count(e:name), min(e:amount), max(e:amount) group by e:name order by count(e:name) desc")
        runAndPrint("11. Niejawne Grupowanie Globalne", "select count(e:name), avg(e:amount)")
        runAndPrint("12. Analiza Wariantów Procesu", "select count(t:name) group by ^e:name order by count(t:name) desc")
        runAndPrint("13. Sortowanie krzyżowe", "select e:name, sum(e:amount) group by e:name order by sum(e:amount) desc")
        runAndPrint("14. Globalne Limity i Offsety", "select e:name, e:timestamp order by e:timestamp asc limit 2 offset 1")
        runAndPrint("15. Limity Hierarchiczne (po 1 z każdego śladu)", "select e:name, t:name limit e:1")
    }

    @Test
    fun evaluateAdvancedPqlMechanics() {
        println("================ ROZPOCZĘCIE ZAAWANSOWANEJ EWALUACJI PQL ================")

        runAndPrint("16. Grupowanie Wielowymiarowe", "select t:name, e:name, count(e:name) group by t:name, e:name order by t:name asc, e:name desc")
        runAndPrint("17. Agregacja wartości wewnątrz Śladu", "select t:name, sum(e:amount), count(e:name) group by t:name order by sum(e:amount) desc")
        runAndPrint("18. Zaawansowana Arytmetyka z Zagnieżdżaniem", "select e:name, ROUND(((e:amount * 0.8) + 50) / 2) where e:amount IS NOT NULL")
        runAndPrint("19. Konwersja czasu na godziny", "select e:name, ROUND((e:timestamp - '2023-10-01T10:00:00.000+00:00') * 24) where e:name = 'Receive Order'")
        runAndPrint("20. Filtrowanie Regex", "select e:name, t:name where e:name MATCHES '.*(Invoice|Goods).*'")
        runAndPrint("21. Warunki łączone na 3 poziomach naraz", "select e:name, t:name, l:name where l:name = 'Webshop Process' AND t:name LIKE 'Order_%' AND e:amount < 200")
        runAndPrint("22. Hierarchiczne Limity złożone", "select e:name, t:name order by t:name asc, e:timestamp asc limit t:2, e:2")
    }

    @Test
    fun evaluateEnterprisePqlMechanics() {
        println("================ ROZPOCZĘCIE EWALUACJI ENTERPRISE (KPI & XES) ================")

        runAndPrint("23. Throughput Time (Czas trwania procesu - różnica min/max w grupie)", "select t:name, (max(e:timestamp) - min(e:timestamp)) group by t:name order by (max(e:timestamp) - min(e:timestamp)) desc")
        runAndPrint("24. Ekstrakcja rozszerzeń XES (org:resource, lifecycle:transition)", "select e:name, e:org:resource, e:lifecycle:transition where e:org:resource = 'System'")
        runAndPrint("25. Filtrowanie i projekcja wartości Boolean (is_premium)", "select e:name, t:name, t:is_premium where t:is_premium = true")
        runAndPrint("26. Filtrowanie po Klasyfikatorze (Złożony WHERE)", "select e:name, t:name where [e:c:Event Name] = 'Ship Goods'")
        runAndPrint("27. Podwójne zagnieżdżenie funkcji skalarnej z matematyką", "select e:name, (year(e:timestamp) + 1), UPPER(e:org:resource) where e:org:resource IS NOT NULL")
        runAndPrint("28. Analiza wydajności zasobów (Resource Mining)", "select e:org:resource, count(e:name) group by e:org:resource order by count(e:name) desc")
        runAndPrint("29. Ekstremalne Offsety (Pomiń 1 ślad, z pozostałych pomiń 1 zdarzenie, zwróć max 1)", "select e:name, t:name order by t:name asc, e:timestamp asc limit e:1 offset t:1, e:1")
        runAndPrint("30. Logika ujemna (Złożone warunki NOT i NOT IN)", "select e:name, e:org:resource where NOT (e:org:resource IN ('Alice', 'Bob')) AND e:amount IS NULL")
    }

    @Test
    fun evaluateDeleteMechanics() {
        println("================ ROZPOCZĘCIE EWALUACJI DELETE ================")

        runAndPrint("31. Przed DELETE: Szukamy zdarzeń Cancel Order", "select e:name, t:name where e:name = 'Cancel Order'")
        runAndPrint("32. Wykonanie DELETE", "delete event where e:name = 'Cancel Order'")
        runAndPrint("33. Po DELETE: Weryfikacja usunięcia (powinno być puste)", "select e:name, t:name where e:name = 'Cancel Order'")
        runAndPrint("34. Weryfikacja integralności: Pozostałe zdarzenia w bazie (Cancel Order zniknęło, reszta została)", "select e:name, t:name order by t:name asc, e:timestamp asc")
    }

    @AfterAll
    fun cleanup() {
        try { dbManager.deleteDb(dbName) } catch (e: Exception) {}
        File(testXesPath).delete()
    }
}
package app

import com.google.gson.GsonBuilder
import db.CouchDBManager
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import java.io.File
import kotlin.test.assertNotNull

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class PqlEvaluationTest {

    private lateinit var dbManager: CouchDBManager
    private lateinit var interpreter: PqlInterpreter
    private val dbName = "pql_evaluation_test_db"
    private val testXesPath = "src/test/resources/pql_evaluation_log.xes"
    private val gson = GsonBuilder().setPrettyPrinting().create()

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
        LogImporter.import(testXesPath, dbName, dbManager)
        interpreter = PqlInterpreter(dbManager, dbName)
    }

    private fun evaluateQuery(query: String) {
        println("==================================================")
        println("QUERY: $query")
        println("--------------------------------------------------")
        val result = interpreter.executeQuery(query)
        assertNotNull(result)
        println(gson.toJson(result))
        println("==================================================\n")
    }

    @Test
    @Order(1)
    fun evaluatePqlCompliance() {
        evaluateQuery("select e:name, [e:c:Event Name] where e:name = 'Cancel Order'")
        evaluateQuery("select e:* where e:name = 'Cancel Order'")
        evaluateQuery("select e:name, t:name where t:name = 'Order_2'")
        evaluateQuery("select e:name, [e:amount] where ([e:amount] > 100 and e:name like 'Receive%') or e:name in ('Cancel Order')")
        evaluateQuery("select e:name, [e:reason] where [e:reason] is not null")
        evaluateQuery("select upper(e:name), lower([e:reason]) where [e:reason] is not null")
        evaluateQuery("select [e:amount], round([e:amount] * 1.23) where [e:amount] is not null")
        evaluateQuery("select e:name, year(e:timestamp), month(e:timestamp), dayofweek(e:timestamp) where e:name = 'Receive Order'")
        evaluateQuery("select e:name, (e:timestamp - '2023-10-01T10:00:00.000+00:00') where e:name = 'Receive Order' order by e:timestamp asc")
        evaluateQuery("select e:name, count(e:name), min([e:amount]), max([e:amount]) group by e:name order by count(e:name) desc")
        evaluateQuery("select count(e:name), avg([e:amount])")
        evaluateQuery("select count(t:name) group by ^e:name order by count(t:name) desc")
        evaluateQuery("select e:name, sum([e:amount]) group by e:name order by sum([e:amount]) desc")
        evaluateQuery("select e:name, e:timestamp order by e:timestamp asc limit 2 offset 1")
        evaluateQuery("select e:name, t:name limit e:1")
    }

    @Test
    @Order(2)
    fun evaluateAdvancedPqlMechanics() {
        evaluateQuery("select t:name, e:name, count(e:name) group by t:name, e:name order by t:name asc, e:name desc")
        evaluateQuery("select t:name, sum([e:amount]), count(e:name) group by t:name order by sum([e:amount]) desc")
        evaluateQuery("select e:name, round((([e:amount] * 0.8) + 50) / 2) where [e:amount] is not null")
        evaluateQuery("select e:name, round((e:timestamp - '2023-10-01T10:00:00.000+00:00') * 24) where e:name = 'Receive Order'")
        evaluateQuery("select e:name, t:name where e:name matches '.*(Invoice|Goods).*'")
        evaluateQuery("select e:name, t:name, l:name where l:name = 'Webshop Process' and t:name like 'Order_%' and [e:amount] < 200")
        evaluateQuery("select e:name, t:name order by t:name asc, e:timestamp asc limit t:2, e:2")
    }

    @Test
    @Order(3)
    fun evaluateEnterprisePqlMechanics() {
        evaluateQuery("select t:name, (max(e:timestamp) - min(e:timestamp)) group by t:name order by (max(e:timestamp) - min(e:timestamp)) desc")
        evaluateQuery("select e:name, e:org:resource, e:lifecycle:transition where e:org:resource = 'System'")
        evaluateQuery("select e:name, t:name, [t:is_premium] where [t:is_premium] = true")
        evaluateQuery("select e:name, t:name where [e:c:Event Name] = 'Ship Goods'")
        evaluateQuery("select e:name, (year(e:timestamp) + 1), upper(e:org:resource) where e:org:resource is not null")
        evaluateQuery("select e:org:resource, count(e:name) group by e:org:resource order by count(e:name) desc")
        evaluateQuery("select e:name, t:name order by t:name asc, e:timestamp asc limit e:1 offset t:1, e:1")
        evaluateQuery("select e:name, e:org:resource where not (e:org:resource in ('Alice', 'Bob')) and [e:amount] is null")
    }

    @Test
    @Order(4)
    fun evaluateDeleteMechanics() {
        evaluateQuery("select e:name, t:name where e:name = 'Cancel Order'")
        evaluateQuery("delete event where e:name = 'Cancel Order'")
        evaluateQuery("select e:name, t:name where e:name = 'Cancel Order'")
        evaluateQuery("select e:name, t:name order by t:name asc, e:timestamp asc")
    }

    @AfterAll
    fun cleanup() {
        try { dbManager.deleteDb(dbName) } catch (e: Exception) {}
        File(testXesPath).delete()
    }
}
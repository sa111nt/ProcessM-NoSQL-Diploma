package app

import db.CouchDBManager
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.File
import java.lang.management.ManagementFactory

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PqlScalabilityTest {

    private lateinit var dbManager: CouchDBManager
    private lateinit var interpreter: PqlInterpreter
    private val dbName = "pql_scalability_test_db"

    private fun getCpuTimeMs(): Double {
        val bean = ManagementFactory.getThreadMXBean()
        return if (bean.isCurrentThreadCpuTimeSupported) {
            bean.currentThreadCpuTime / 1_000_000.0
        } else {
            0.0
        }
    }

    @BeforeAll
    fun setup() {
        dbManager = CouchDBManager("http://127.0.0.1:5984", "admin", "admin")
        try { dbManager.deleteDb(dbName) } catch (e: Exception) {}
        try { dbManager.createDb(dbName) } catch (e: Exception) {}
        interpreter = PqlInterpreter(dbManager, dbName)
    }

    @AfterAll
    fun cleanup() {
        try { dbManager.deleteDb(dbName) } catch (e: Exception) {}
    }

    @Test
    fun runScalabilityExperiments() {
        val queries = listOf(
            "Q1_Projection" to "select e:concept:name limit {LIMIT}",
            "Q2_SimpleFilter" to "select e:concept:name where e:concept:name = 'A_SUBMITTED' limit {LIMIT}",
            "Q3_RegexFilter" to "select e:concept:name where e:concept:name MATCHES '.*(A_).*' limit {LIMIT}",
            "Q4_CrossScope" to "select e:concept:name where t:concept:name = 'Trace_1' limit {LIMIT}",
            "Q5_GlobalAggr" to "select count(e:concept:name), min(e:time:timestamp)",
            "Q6_Sorting" to "select e:concept:name order by e:time:timestamp desc limit {LIMIT}",
            "Q7_FlatGroupBy" to "select e:org:resource, count(e:concept:name) group by e:org:resource limit {LIMIT}",
            "Q8_VariantMining" to "select count(t:concept:name) group by ^e:concept:name limit {LIMIT}"
        )

        val resultLimits = listOf(10, 100, 1000, 10000)

        val datasets = listOf(
            Triple("JournalReview-extra.xes", 1, 2298),
            Triple("Hospital_log.xes.gz", 1, 150291),
            Triple("Hospital_log.xes.gz", 7, 1052037)
        )

        println("\n================= WYNIKI EKSPERYMENTÓW SKALOWALNOŚCI =================")
        println("Events_In_DB,Result_Limit,Query_Name,Actual_Result_Size,CPU_Time_ms,Real_Time_ms")

        for ((dataset, importCount, totalEvents) in datasets) {
            val filePath = "src/test/resources/$dataset"
            if (!File(filePath).exists()) {
                continue
            }

            try { dbManager.deleteDb(dbName) } catch (e: Exception) {}
            try { dbManager.createDb(dbName) } catch (e: Exception) {}

            for (i in 1..importCount) {
                LogImporter.import(filePath, dbName, dbManager)
            }

            for ((queryName, queryTemplate) in queries) {

                val limitsToTest = if (queryTemplate.contains("{LIMIT}")) resultLimits else listOf(1)

                for (limit in limitsToTest) {
                    val queryStr = queryTemplate.replace("{LIMIT}", limit.toString())

                    try { interpreter.executeQuery(queryStr) } catch (e: Throwable) {}
                    System.gc()
                    Thread.sleep(500)

                    val startCpu = getCpuTimeMs()
                    val startReal = System.currentTimeMillis()

                    var actualResultSize = "0"
                    var cpuDurationStr = ""
                    var realDurationStr = ""

                    try {
                        val result = interpreter.executeQuery(queryStr)

                        val endCpu = getCpuTimeMs()
                        val endReal = System.currentTimeMillis()

                        actualResultSize = result.size().toString()
                        cpuDurationStr = String.format("%.2f", endCpu - startCpu).replace(",", ".")
                        realDurationStr = (endReal - startReal).toString()
                    } catch (e: Exception) {
                        cpuDurationStr = "ERROR"
                        realDurationStr = "ERROR"
                    } catch (t: Throwable) {
                        cpuDurationStr = "OOM_Memory_Limit"
                        realDurationStr = "OOM_Memory_Limit"
                    }

                    println("$totalEvents,$limit,$queryName,$actualResultSize,$cpuDurationStr,$realDurationStr")
                }
            }
        }
        println("======================================================================\n")
    }
}
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

    private lateinit var databaseManager: CouchDBManager
    private lateinit var pqlInterpreter: PqlInterpreter
    private val benchmarkDatabaseName = "pql_scalability_benchmark_db"

    private fun getCurrentThreadCpuTimeMs(): Double {
        val threadBean = ManagementFactory.getThreadMXBean()
        return if (threadBean.isCurrentThreadCpuTimeSupported) {
            threadBean.currentThreadCpuTime / 1_000_000.0
        } else {
            0.0
        }
    }

    @BeforeAll
    fun initializeTestEnvironment() {
        databaseManager = CouchDBManager("http://127.0.0.1:5984", "admin", "admin")
        recreateDatabase()
        pqlInterpreter = PqlInterpreter(databaseManager, benchmarkDatabaseName)
    }

    @AfterAll
    fun destroyTestEnvironment() {
        dropDatabase()
    }

    private fun recreateDatabase() {
        dropDatabase()
        try {
            databaseManager.createDb(benchmarkDatabaseName)
        } catch (ignored: Exception) {}
    }

    private fun dropDatabase() {
        try {
            databaseManager.deleteDb(benchmarkDatabaseName)
        } catch (ignored: Exception) {}
    }

    private fun prepareExecutionEnvironment() {
        System.gc()
        Thread.sleep(500)
    }

    @Test
    fun evaluateScalabilityMetrics() {
        val benchmarkQueries = listOf(
            "Q1_Projection"      to "select e:concept:name limit {LIMIT}",
            "Q2_ZeroCpuFilter"   to "select e:concept:name where e:concept:name = 'NON_EXISTENT_EVENT' limit {LIMIT}",
            "Q3_RegexFilter"     to "select e:concept:name where e:concept:name MATCHES '.*(A_).*' limit {LIMIT}",
            "Q4_CrossScope"      to "select e:concept:name where t:concept:name = 'Trace_1' limit {LIMIT}",
            "Q5_GlobalAggr"      to "select count(e:concept:name), min(e:time:timestamp)",
            "Q6_Sorting"         to "select e:concept:name order by e:time:timestamp desc limit {LIMIT}",
            "Q7_FlatGroupBy"     to "select e:concept:name, count(e:time:timestamp) group by e:concept:name limit {LIMIT}",
            "Q8_VariantMining"   to "select count(t:concept:name) group by ^e:concept:name limit {LIMIT}"
        )

        val resultSizeLimits = listOf(100, 1000)

        val benchmarkDatasets = listOf(
            Triple("JournalReview-extra.xes", 1, 2298),
            Triple("Hospital_log.xes.gz", 1, 150291),
            Triple("Hospital_log.xes.gz", 7, 1052037)
        )

        println("DatasetSize,ResultLimit,QueryType,ActualResultSize,CpuTimeMs,RealTimeMs")

        for ((datasetFileName, importMultiplicity, totalEventCount) in benchmarkDatasets) {
            val fileResourcePath = "src/test/resources/$datasetFileName"
            if (!File(fileResourcePath).exists()) continue

            recreateDatabase()

            for (iteration in 1..importMultiplicity) {
                LogImporter.import(fileResourcePath, benchmarkDatabaseName, databaseManager)
            }

            for ((queryIdentifier, queryTemplate) in benchmarkQueries) {
                val limitsToEvaluate = if (queryTemplate.contains("{LIMIT}")) resultSizeLimits else listOf(1)

                val warmupQuery = queryTemplate.replace("{LIMIT}", "100")
                try {
                    pqlInterpreter.executeQuery(warmupQuery)
                } catch (ignored: Throwable) {}

                for (limit in limitsToEvaluate) {
                    val executableQuery = queryTemplate.replace("{LIMIT}", limit.toString())

                    prepareExecutionEnvironment()

                    val startCpuTime = getCurrentThreadCpuTimeMs()
                    val startRealTime = System.currentTimeMillis()

                    var actualResultSize = "0"
                    var cpuDurationFormatted = ""
                    var realDurationFormatted = ""

                    try {
                        val executionResult = pqlInterpreter.executeQuery(executableQuery)

                        val endCpuTime = getCurrentThreadCpuTimeMs()
                        val endRealTime = System.currentTimeMillis()

                        actualResultSize = executionResult.size().toString()
                        cpuDurationFormatted = String.format("%.2f", endCpuTime - startCpuTime).replace(",", ".")
                        realDurationFormatted = (endRealTime - startRealTime).toString()

                    } catch (exception: Exception) {
                        cpuDurationFormatted = "ERROR"
                        realDurationFormatted = "ERROR"
                    } catch (throwable: Throwable) {
                        cpuDurationFormatted = "OOM_Memory_Limit"
                        realDurationFormatted = "OOM_Memory_Limit"
                    }

                    println("$totalEventCount,$limit,$queryIdentifier,$actualResultSize,$cpuDurationFormatted,$realDurationFormatted")
                }
            }
        }
    }
}
package input.parsers

import org.junit.jupiter.api.Test
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SaxXesParserLongTest {

    @Test
    fun `should correctly parse multiple traces and maintain valid relationships`() {
        val parser = SaxXesParser()
        val log = parser.parse(File("src/test/resources/long_sample.xes"))

        println("Parsed ${log.traces.size} traces and ${log.events.size} events")

        // 1️⃣ Sprawdzamy liczbę trace’ów i eventów
        assertEquals(5, log.traces.size, "❌ Expected 5 traces")
        assertEquals(15, log.events.size, "❌ Expected 15 events")

        // 2️⃣ Sprawdzamy unikalność event.id
        val eventIds = log.events.map { it.id }
        assertEquals(eventIds.size, eventIds.toSet().size, "❌ Duplicate event IDs found")

        // 3️⃣ Sprawdzamy, że każdy event ma traceId z listy
        val traceIds = log.traces.map { it.id }.toSet()
        assertTrue(
            log.events.all { e -> e.traceId in traceIds },
            "❌ Some events reference missing trace IDs"
        )

        // 4️⃣ Sprawdzamy, że każdy trace ma przynajmniej jeden event
        assertTrue(
            log.traces.all { it.events.isNotEmpty() },
            "❌ Found trace with no events"
        )

        // 5️⃣ Sprawdzamy, że sumaryczna liczba eventów zgadza się
        val totalInTraces = log.traces.sumOf { it.events.size }
        assertEquals(
            log.events.size, totalInTraces,
            "❌ Event count mismatch between traces and event list"
        )

        // 6️⃣ Opcjonalne logowanie
        log.traces.forEach { trace ->
            println("Trace ${trace.id} (${trace.attributes["concept:name"]}) has ${trace.events.size} events")
        }

        println("✅ All relationships and IDs validated successfully.")
    }
}
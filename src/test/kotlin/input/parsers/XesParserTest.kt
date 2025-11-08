package input.parsers

import org.junit.jupiter.api.Test
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SaxXesParserTest {

    @Test
    fun `should generate unique event IDs and maintain correct trace relations`() {
        val parser = SaxXesParser()
        val log = parser.parse(File("src/test/resources/sample.xes"))

        println("Parsed ${log.traces.size} traces and ${log.events.size} events")

        // 1️⃣ Unikalność ID eventów
        val eventIds = log.events.map { it.id }
        assertEquals(eventIds.size, eventIds.toSet().size, "❌ Duplicate event IDs detected")

        // 2️⃣ Każdy event ma traceId, który istnieje
        val traceIds = log.traces.map { it.id }.toSet()
        assertTrue(
            log.events.all { e -> e.traceId in traceIds },
            "❌ Some events reference missing trace IDs"
        )

        // 3️⃣ Zliczanie — liczba eventów w trace’ach powinna się zgadzać
        val totalInTraces = log.traces.sumOf { it.events.size }
        assertEquals(log.events.size, totalInTraces, "❌ Event count mismatch between traces and global list")

        println("✅ All event IDs are unique and trace relations are correct.")
    }
}
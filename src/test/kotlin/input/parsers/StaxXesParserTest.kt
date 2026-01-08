package input.parsers

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.DisplayName
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import javax.xml.stream.XMLInputFactory

class StaxXesParserTest {

    // Helper: Tworzy parser z ciągu znaków
    private fun createParser(xml: String): StaxXesParser {
        val stream = ByteArrayInputStream(xml.toByteArray(StandardCharsets.UTF_8))
        val reader = XMLInputFactory.newDefaultFactory().createXMLStreamReader(stream)
        return StaxXesParser(reader)
    }

    @Test
    @DisplayName("Powinien poprawnie odczytać atrybuty logu")
    fun testLogAttributes() {
        val xml = """
            <log xes.version="1.0">
                <string key="concept:name" value="TestLog"/>
                <string key="source" value="JUnit"/>
            </log>
        """.trimIndent()

        val parser = createParser(xml)

        assertTrue(parser.hasNext())
        val component = parser.next()

        assertTrue(component is XESLogAttributes)
        val attrs = (component as XESLogAttributes).attributes
        assertEquals("TestLog", attrs["concept:name"])
        assertEquals("JUnit", attrs["source"])
    }

    @Test
    @DisplayName("Powinien poprawnie odczytać Trace i jego Eventy")
    fun testTraceParsing() {
        val xml = """
            <log>
                <trace>
                    <string key="concept:name" value="Case1"/>
                    <event>
                        <string key="concept:name" value="Activity A"/>
                        <date key="time:timestamp" value="2023-10-01T12:00:00Z"/>
                        <int key="cost" value="100"/>
                    </event>
                </trace>
            </log>
        """.trimIndent()

        val parser = createParser(xml)
        parser.next() // Skip log attributes

        assertTrue(parser.hasNext())
        val traceComp = parser.next()
        assertTrue(traceComp is XESTrace)

        val trace = (traceComp as XESTrace).trace
        assertEquals("Case1", trace.attributes["concept:name"])
        assertEquals(1, trace.events.size)

        val event = trace.events[0]
        assertEquals("Activity A", event.name)
        assertEquals("2023-10-01T12:00:00Z", event.timestamp)
        // Sprawdzenie rzutowania typów (int)
        assertEquals(100L, event.attributes["cost"])
    }

    @Test
    @DisplayName("Powinien parsować wiele śladów sekwencyjnie")
    fun testMultipleTraces() {
        val xml = """
            <log>
                <trace><string key="id" value="1"/></trace>
                <trace><string key="id" value="2"/></trace>
            </log>
        """.trimIndent()

        val parser = createParser(xml)
        parser.next() // log attrs

        val t1 = (parser.next() as XESTrace).trace
        assertEquals("1", t1.attributes["id"])

        val t2 = (parser.next() as XESTrace).trace
        assertEquals("2", t2.attributes["id"])

        assertFalse(parser.hasNext())
    }
}
package pql

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.Assertions.assertEquals
import pql.model.*
import pql.parser.AntlrPqlParser

/**
 * Testy negatywne sprawdzające reakcję parsera na błędną składnię
 * i nieobsługiwane konstrukcje.
 */
class PqlNegativeTest {
    private val parser = AntlrPqlParser()

    @Test
    @DisplayName("Should throw exception for unclosed string literal")
    fun testUnclosedString() {
        val pql = "SELECT * WHERE e:concept:name = 'Unclosed string"
        assertThrows<IllegalArgumentException> {
            parser.parse(pql)
        }
    }

    @Test
    @DisplayName("Should fallback to EVENT scope for invalid prefix")
    fun testInvalidScope() {
        // Zamiast rzucać błędem, system ustawia fallback na PqlScope.EVENT
        val pql = "SELECT x:attribute"
        val query = parser.parse(pql) as PqlQuery
        assertEquals(PqlScope.EVENT, query.projections[0].scope)
        assertEquals("x:attribute", query.projections[0].attribute)
    }

    @Test
    @DisplayName("Should throw exception for trailing comma in IN list")
    fun testInvalidInList() {
        val pql = "SELECT * WHERE e:id IN ('A', 'B', )"
        assertThrows<IllegalArgumentException> {
            parser.parse(pql)
        }
    }

    @Test
    @DisplayName("Should throw exception for missing mandatory SELECT")
    fun testMissingSelect() {
        val pql = "e:concept:name = 'A'"
        assertThrows<IllegalArgumentException> {
            parser.parse(pql)
        }
    }

    @Test
    @DisplayName("Should throw exception for malformed arithmetic expression")
    fun testMalformedArithmetic() {
        val pql = "SELECT e:cost + * 5"
        assertThrows<IllegalArgumentException> {
            parser.parse(pql)
        }
    }

    @Test
    @DisplayName("Should throw exception for unknown function name")
    fun testUnknownFunction() {
        val pql = "SELECT nonExistentFunction(e:id)"
        // Zależnie od implementacji listenera, może przejść przez parser ANTLR
        // ale wyrzucić błąd podczas budowania modelu w listenerze
        assertThrows<IllegalArgumentException> {
            parser.parse(pql)
        }
    }
}
package pql.parser

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.assertThrows

// DODANE IMPORTY NAPRAWIAJĄCE BŁĘDY:
import pql.model.*

class PqlParserLogicTest {
    private val parser = AntlrPqlParser()

    @Test
    @DisplayName("LiteralTests: escapeCharInStringTest & specialEscapeCharactersTest")
    fun testStringLiteralsAndEscapes() {
        val pql = "SELECT * WHERE e:name = \"abc jr\\\"\" AND e:activity = \"\\t\\r\\n\""
        val query = parser.parse(pql) as PqlQuery

        // Rzutowanie wymaga zaimportowanych klas PqlCondition i PqlCondition.And/Simple
        val andCond = query.conditions[0] as PqlCondition.And
        val cond1 = andCond.conditions[0] as PqlCondition.Simple
        val cond2 = andCond.conditions[1] as PqlCondition.Simple

        assertEquals("abc jr\"", cond1.value)
        assertEquals("\t\r\n", cond2.value)
    }

    @Test
    @DisplayName("QueryTests: basicSelectTest & selectAggregationTest")
    fun testProjectionsAndScopes() {
        val pql = "SELECT l:name, t:name, e:name, min(t:total), avg(e:duration)"
        val query = parser.parse(pql) as PqlQuery

        assertEquals(PqlScope.LOG, query.projections[0].scope)
        assertEquals(PqlScope.TRACE, query.projections[1].scope)
        assertEquals(PqlScope.EVENT, query.projections[2].scope)

        val minFunc = query.projections[3].function as PqlFunction.AggregationFunction
        assertEquals(PqlFunction.Aggregation.MIN, minFunc.type)

        val avgFunc = query.projections[4].function as PqlFunction.AggregationFunction
        assertEquals(PqlFunction.Aggregation.AVG, avgFunc.type)
    }

    @Test
    @DisplayName("QueryTests: scopedSelectAll2Test")
    fun testSelectAllMapping() {
        val pql = "SELECT t:*, e:*, l:*"
        val query = parser.parse(pql) as PqlQuery
        assertTrue(query.selectAll)
    }

    @Test
    @DisplayName("AttributeTests: unicodeCustomAttributeTest")
    fun testUnicodeAndSpecialAttributes() {
        // PQL pozwala na atrybuty w nawiasach [] ze znakami specjalnymi
        val pql = "SELECT [e:Ոչ ոք չի սիրում], [t:!@#$%^&*()]"
        val query = parser.parse(pql) as PqlQuery

        // Weryfikujemy czy projekcje zostały dodane
        assertEquals(2, query.projections.size)

        // POPRAWKA LITERÓWKI: Upewnij się, że znaki są identyczne jak w pql stringu
        assertEquals("Ոչ ոք չի սիրում", query.projections[0].attribute)
        assertEquals("!@#\$%^&*()", query.projections[1].attribute)
    }

    @Test
    @DisplayName("FunctionTests: validScalarFunctionTest")
    fun testScalarFunctions() {
        val pql = "SELECT year(e:timestamp)"
        val query = parser.parse(pql) as PqlQuery

        val func = query.projections[0].function as PqlFunction.ScalarFunction1
        assertEquals(PqlFunction.Scalar1.YEAR, func.type)
    }

    @Test
    @DisplayName("OrderDirection & Scope: Walidacja enumów")
    fun testEnums() {
        assertEquals(PqlScope.LOG, PqlScope.fromToken("l"))
        assertEquals(PqlScope.TRACE, PqlScope.fromToken("trace"))

        assertThrows<IllegalStateException> { PqlScope.fromToken("XYZ") }
    }
}
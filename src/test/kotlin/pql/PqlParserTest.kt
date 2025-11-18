package pql

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class PqlParserTest {

    private val parser = PqlParser()

    @Test
    fun `parses basic event query`() {
        val query = parser.parse(
            """
            select e:concept:name, e:timestamp
            from events
            where e:concept:name = 'register request'
            order by e:timestamp desc
            limit 5
            """.trimIndent()
        )

        assertEquals(PqlScope.EVENT, query.collection)
        assertEquals(2, query.projections.size)
        assertFalse(query.selectAll)
        assertEquals(1, query.conditions.size)
        val condition = query.conditions.first()
        assertEquals(PqlScope.EVENT, condition.scope)
        assertEquals("concept:name", condition.attribute)
        assertEquals(PqlOperator.EQ, condition.operator)
        assertEquals("register request", condition.value)
        assertNotNull(query.orderBy)
        assertEquals(SortDirection.DESC, query.orderBy?.direction)
        assertEquals(5, query.limit)
    }

    @Test
    fun `select star toggles selectAll`() {
        val query = parser.parse("select * from log limit 1")
        assertTrue(query.selectAll)
        assertEquals(PqlScope.LOG, query.collection)
    }
}


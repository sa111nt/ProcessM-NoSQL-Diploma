package pql.model

data class PqlQuery(
    val collection: PqlScope,
    val projections: List<PqlProjection>,
    val conditions: List<PqlCondition> = emptyList(),
    val orderBy: List<PqlOrder> = emptyList(),
    val limit: Int? = null,
    val offset: Int? = null,
    val limits: Map<PqlScope, Int> = emptyMap(),
    val offsets: Map<PqlScope, Int> = emptyMap(),
    val groupBy: List<PqlGroupByField> = emptyList(),
    val selectAll: Boolean = false
)

data class PqlGroupByField(
    val scope: PqlScope,
    val attribute: String
)

data class PqlProjection(
    val scope: PqlScope?,
    val attribute: String?,
    val alias: String? = null,
    val allAttributes: Boolean = false,
    val raw: String,
    val function: PqlFunction? = null,
    val arithmeticExpression: PqlArithmeticExpression? = null
)

sealed class PqlArithmeticExpression {
    data class Binary(
        val left: PqlArithmeticExpression,
        val operator: ArithmeticOperator,
        val right: PqlArithmeticExpression
    ) : PqlArithmeticExpression()

    data class Attribute(
        val scope: PqlScope,
        val attribute: String
    ) : PqlArithmeticExpression()

    data class Literal(
        val value: Any
    ) : PqlArithmeticExpression()

    data class FunctionCall(
        val function: PqlFunction
    ) : PqlArithmeticExpression()
}

enum class ArithmeticOperator { ADD, SUB, MUL, DIV }

sealed class PqlFunction {
    enum class Aggregation { COUNT, SUM, AVG, MIN, MAX }
    enum class Scalar1 { DATE, TIME, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, QUARTER, DAYOFWEEK, UPPER, LOWER, ROUND }
    enum class Scalar0 { NOW }

    data class AggregationFunction(val type: Aggregation, val scope: PqlScope?, val attribute: String?) : PqlFunction()

    // ZMIANA: Model utrzymuje pełne drzewo AST operacji matematycznych
    data class ScalarFunction1(val type: Scalar1, val expression: PqlArithmeticExpression) : PqlFunction()
    data class ScalarFunction0(val type: Scalar0) : PqlFunction()
}

sealed class PqlCondition {
    data class Simple(val scope: PqlScope, val attribute: String, val operator: PqlOperator, val value: Any?) : PqlCondition()
    data class And(val conditions: List<PqlCondition>) : PqlCondition()
    data class Or(val conditions: List<PqlCondition>) : PqlCondition()
    data class Not(val condition: PqlCondition) : PqlCondition()
    data object AlwaysFalse : PqlCondition()
}

enum class PqlOperator {
    EQ, NEQ, LT, LE, GT, GE, IN, NOT_IN, LIKE, MATCHES, IS_NULL, IS_NOT_NULL
}

data class PqlOrder(
    val scope: PqlScope,
    val attribute: String,
    val direction: SortDirection,
    val function: PqlFunction? = null,
    val arithmeticExpression: PqlArithmeticExpression? = null
)

enum class SortDirection { ASC, DESC }

enum class PqlScope(val aliases: Set<String>, val docType: String, val label: String) {
    LOG(setOf("log", "logs", "l"), "log", "log"),
    TRACE(setOf("trace", "traces", "t"), "trace", "trace"),
    EVENT(setOf("event", "events", "e"), "event", "event");

    companion object {
        fun fromToken(token: String): PqlScope {
            val normalized = token.trim()
            return entries.firstOrNull { scope ->
                scope.aliases.any { it.equals(normalized, ignoreCase = true) }
            } ?: error("Unknown scope: '$token'")
        }
    }
}

data class PqlDeleteQuery(
    val scope: PqlScope? = null,
    val conditions: List<PqlCondition> = emptyList(),
    val orderBy: List<PqlOrder> = emptyList(),
    val limit: Int? = null,
    val offset: Int? = null
)
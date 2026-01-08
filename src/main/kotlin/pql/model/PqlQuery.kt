package pql.model

data class PqlQuery(
    val collection: PqlScope,
    val projections: List<PqlProjection>,
    val conditions: List<PqlCondition> = emptyList(),
    val orderBy: PqlOrder? = null,
    val limit: Int? = null,
    val offset: Int? = null,
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
    val function: PqlFunction? = null, // Function applied to this projection
    val arithmeticExpression: PqlArithmeticExpression? = null // Arithmetic expression (e.g., e:cost + 100)
)

// Represents an arithmetic expression
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
        val value: Any // Number or String
    ) : PqlArithmeticExpression()
    
    data class FunctionCall(
        val function: PqlFunction
    ) : PqlArithmeticExpression()
}

enum class ArithmeticOperator {
    ADD,    // +
    SUB,    // -
    MUL,    // *
    DIV     // /
}

// Represents a function call in PQL
sealed class PqlFunction {
    // Aggregation functions
    enum class Aggregation {
        COUNT, SUM, AVG, MIN, MAX
    }
    
    // Scalar functions with 1 argument
    enum class Scalar1 {
        DATE, TIME, YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND,
        QUARTER, DAYOFWEEK, UPPER, LOWER, ROUND
    }
    
    // Scalar function with 0 arguments
    enum class Scalar0 {
        NOW
    }
    
    data class AggregationFunction(
        val type: Aggregation,
        val scope: PqlScope?,
        val attribute: String?
    ) : PqlFunction()
    
    data class ScalarFunction1(
        val type: Scalar1,
        val scope: PqlScope?,
        val attribute: String?
    ) : PqlFunction()
    
    data class ScalarFunction0(
        val type: Scalar0
    ) : PqlFunction()
}

// Represents a condition in WHERE clause
sealed class PqlCondition {
    // Simple condition: attribute operator value
    data class Simple(
        val scope: PqlScope,
        val attribute: String,
        val operator: PqlOperator,
        val value: Any? // String, Number, List<String> for IN, null for IS NULL
    ) : PqlCondition()
    
    // Complex conditions with logical operators
    data class And(val conditions: List<PqlCondition>) : PqlCondition()
    data class Or(val conditions: List<PqlCondition>) : PqlCondition()
    data class Not(val condition: PqlCondition) : PqlCondition()
}

enum class PqlOperator {
    // Comparison operators
    EQ,      // =
    NEQ,     // !=
    LT,      // <
    LE,      // <=
    GT,      // >
    GE,      // >=
    IN,      // IN
    NOT_IN,  // NOT IN
    LIKE,    // LIKE
    MATCHES, // matches
    IS_NULL, // IS NULL
    IS_NOT_NULL // IS NOT NULL
}

data class PqlOrder(
    val scope: PqlScope,
    val attribute: String,
    val direction: SortDirection
)

enum class SortDirection {
    ASC,
    DESC
}

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

// Represents a DELETE query
data class PqlDeleteQuery(
    val scope: PqlScope? = null, // Optional scope (e, t, l) - if null, deletes from all scopes
    val conditions: List<PqlCondition> = emptyList(),
    val orderBy: PqlOrder? = null,
    val limit: Int? = null,
    val offset: Int? = null
)


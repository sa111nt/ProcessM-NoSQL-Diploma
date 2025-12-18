package pql

import org.antlr.v4.runtime.tree.ParseTreeWalker
import ql.QLParser
import ql.QLParserBaseListener

/**
 * Listener that processes ANTLR parse tree and builds PqlQuery.
 * This is a simplified version that handles basic queries.
 */
class PqlQueryListener : QLParserBaseListener() {
    
    private var selectAll: Boolean = false
    private val projections = mutableListOf<PqlProjection>()
    private val conditions = mutableListOf<PqlCondition>()
    private var orderBy: PqlOrder? = null
    private var limit: Int? = null
    private var offset: Int? = null
    private val groupByFields = mutableListOf<PqlGroupByField>()
    // Track current scope from attributes (e:, t:, l:)
    private var detectedScope: PqlScope? = null
    
    // DELETE query fields
    private var isDeleteQuery: Boolean = false
    private var deleteScope: PqlScope? = null
    
    override fun enterSelect_all_implicit(ctx: QLParser.Select_all_implicitContext?) {
        selectAll = true
    }
    
    override fun enterSelect_all(ctx: QLParser.Select_allContext?) {
        selectAll = true
    }
    
    override fun enterScoped_select_all(ctx: QLParser.Scoped_select_allContext?) {
        // Handle scoped select all (e.g., select e:*, t:*)
        val scopeToken = ctx?.SCOPE()?.text
        if (scopeToken != null) {
            val scope = PqlScope.fromToken(scopeToken)
            detectedScope = scope
            selectAll = true
        }
    }
    
    override fun exitColumn_list_arith_expr_root(ctx: QLParser.Column_list_arith_expr_rootContext?) {
        val expr = ctx?.arith_expr_root() ?: return
        val arithExpr = expr.arith_expr()
        
        // Check if it's a function call
        val func = arithExpr.func()
        if (func != null) {
            val function = parseFunction(func)
            if (function != null) {
                detectedScope = when (function) {
                    is PqlFunction.AggregationFunction -> function.scope
                    is PqlFunction.ScalarFunction1 -> function.scope
                    is PqlFunction.ScalarFunction0 -> null
                }
                projections.add(
                    PqlProjection(
                        scope = detectedScope,
                        attribute = when (function) {
                            is PqlFunction.AggregationFunction -> function.attribute
                            is PqlFunction.ScalarFunction1 -> function.attribute
                            is PqlFunction.ScalarFunction0 -> null
                        },
                        raw = expr.text,
                        function = function
                    )
                )
                return
            }
        }
        
        // Check if it's an arithmetic expression (has operators)
        val hasArithmetic = arithExpr.children?.any { 
            it is org.antlr.v4.runtime.tree.TerminalNode && 
            (it.text == "+" || it.text == "-" || it.text == "*" || it.text == "/")
        } ?: false
        
        if (hasArithmetic) {
            val arithmeticExpr = parseArithmeticExpression(arithExpr)
            if (arithmeticExpr != null) {
                val scope = extractScopeFromArithmetic(arithmeticExpr)
                scope?.let { detectedScope = it }
                projections.add(
                    PqlProjection(
                        scope = scope,
                        attribute = null,
                        raw = expr.text,
                        arithmeticExpression = arithmeticExpr
                    )
                )
                return
            }
        }
        
        // Regular attribute
        val attribute = parseAttribute(arithExpr)
        if (attribute != null) {
            val scope = attribute.first
            val attrName = attribute.second
            detectedScope = scope
            projections.add(
                PqlProjection(
                    scope = scope,
                    attribute = attrName,
                    raw = expr.text
                )
            )
        }
    }
    
    private fun parseArithmeticExpression(ctx: QLParser.Arith_exprContext): PqlArithmeticExpression? {
        // Check for binary operators (order matters: *, / before +, -)
        val children = ctx.children ?: return null
        
        // Check for multiplication or division
        for (i in children.indices) {
            val child = children[i]
            if (child is org.antlr.v4.runtime.tree.TerminalNode) {
                val text = child.text
                if (text == "*" || text == "/") {
                    val left = ctx.arith_expr(0)
                    val right = ctx.arith_expr(1)
                    val leftExpr = parseArithmeticExpression(left) ?: return null
                    val rightExpr = parseArithmeticExpression(right) ?: return null
                    val operator = if (text == "*") ArithmeticOperator.MUL else ArithmeticOperator.DIV
                    return PqlArithmeticExpression.Binary(leftExpr, operator, rightExpr)
                }
            }
        }
        
        // Check for addition or subtraction
        for (i in children.indices) {
            val child = children[i]
            if (child is org.antlr.v4.runtime.tree.TerminalNode) {
                val text = child.text
                if (text == "+" || text == "-") {
                    val left = ctx.arith_expr(0)
                    val right = ctx.arith_expr(1)
                    val leftExpr = parseArithmeticExpression(left) ?: return null
                    val rightExpr = parseArithmeticExpression(right) ?: return null
                    val operator = if (text == "+") ArithmeticOperator.ADD else ArithmeticOperator.SUB
                    return PqlArithmeticExpression.Binary(leftExpr, operator, rightExpr)
                }
            }
        }
        
        // Check for parentheses
        if (ctx.arith_expr().size == 1 && ctx.text.startsWith("(") && ctx.text.endsWith(")")) {
            return parseArithmeticExpression(ctx.arith_expr(0))
        }
        
        // Check for function
        val func = ctx.func()
        if (func != null) {
            val function = parseFunction(func) ?: return null
            return PqlArithmeticExpression.FunctionCall(function)
        }
        
        // Check for ID (attribute)
        val id = ctx.ID()
        if (id != null) {
            val (scope, attribute) = parseScopeAndAttribute(id.text) ?: return null
            return PqlArithmeticExpression.Attribute(scope, attribute)
        }
        
        // Check for scalar (literal)
        val scalar = ctx.scalar()
        if (scalar != null) {
            val value = when {
                scalar.NUMBER() != null -> {
                    val numStr = scalar.NUMBER()!!.text
                    numStr.toDoubleOrNull() ?: numStr.toLongOrNull() ?: numStr
                }
                scalar.STRING() != null -> scalar.STRING()!!.text.trim('"', '\'')
                scalar.BOOLEAN() != null -> scalar.BOOLEAN()!!.text.toBoolean()
                else -> null
            }
            if (value != null) {
                return PqlArithmeticExpression.Literal(value)
            }
        }
        
        return null
    }
    
    private fun extractScopeFromArithmetic(expr: PqlArithmeticExpression): PqlScope? {
        return when (expr) {
            is PqlArithmeticExpression.Attribute -> expr.scope
            is PqlArithmeticExpression.Binary -> {
                extractScopeFromArithmetic(expr.left) ?: extractScopeFromArithmetic(expr.right)
            }
            is PqlArithmeticExpression.FunctionCall -> {
                when (val func = expr.function) {
                    is PqlFunction.AggregationFunction -> func.scope
                    is PqlFunction.ScalarFunction1 -> func.scope
                    is PqlFunction.ScalarFunction0 -> null
                }
            }
            is PqlArithmeticExpression.Literal -> null
        }
    }
    
    private fun parseFunction(ctx: QLParser.FuncContext): PqlFunction? {
        // Aggregation function: FUNC_AGGR '(' ID ')'
        val funcAggr = ctx.FUNC_AGGR()
        if (funcAggr != null) {
            val id = ctx.ID()?.text ?: return null
            val (scope, attribute) = parseScopeAndAttribute(id) ?: return null
            
            val funcName = funcAggr.text.lowercase()
            val aggregationType = when {
                funcName.contains("count") -> PqlFunction.Aggregation.COUNT
                funcName.contains("sum") -> PqlFunction.Aggregation.SUM
                funcName.contains("avg") -> PqlFunction.Aggregation.AVG
                funcName.contains("min") -> PqlFunction.Aggregation.MIN
                funcName.contains("max") -> PqlFunction.Aggregation.MAX
                else -> return null
            }
            
            return PqlFunction.AggregationFunction(aggregationType, scope, attribute)
        }
        
        // Scalar function with 0 arguments: FUNC_SCALAR0 '(' ')'
        val funcScalar0 = ctx.FUNC_SCALAR0()
        if (funcScalar0 != null) {
            val funcName = funcScalar0.text.lowercase()
            val scalarType = when {
                funcName.contains("now") -> PqlFunction.Scalar0.NOW
                else -> return null
            }
            return PqlFunction.ScalarFunction0(scalarType)
        }
        
        // Scalar function with 1 argument: FUNC_SCALAR1 '(' arith_expr ')'
        val funcScalar1 = ctx.FUNC_SCALAR1()
        if (funcScalar1 != null) {
            val arithExpr = ctx.arith_expr() ?: return null
            val attribute = parseAttribute(arithExpr)
            val (scope, attrName) = attribute ?: return null
            
            val funcName = funcScalar1.text.lowercase()
            val scalarType = when {
                funcName.contains("date") -> PqlFunction.Scalar1.DATE
                funcName.contains("time") -> PqlFunction.Scalar1.TIME
                funcName.contains("year") -> PqlFunction.Scalar1.YEAR
                funcName.contains("month") -> PqlFunction.Scalar1.MONTH
                funcName.contains("day") && !funcName.contains("dayofweek") -> PqlFunction.Scalar1.DAY
                funcName.contains("hour") -> PqlFunction.Scalar1.HOUR
                funcName.contains("minute") -> PqlFunction.Scalar1.MINUTE
                funcName.contains("second") -> PqlFunction.Scalar1.SECOND
                funcName.contains("millisecond") -> PqlFunction.Scalar1.MILLISECOND
                funcName.contains("quarter") -> PqlFunction.Scalar1.QUARTER
                funcName.contains("dayofweek") -> PqlFunction.Scalar1.DAYOFWEEK
                funcName.contains("upper") -> PqlFunction.Scalar1.UPPER
                funcName.contains("lower") -> PqlFunction.Scalar1.LOWER
                funcName.contains("round") -> PqlFunction.Scalar1.ROUND
                else -> return null
            }
            
            return PqlFunction.ScalarFunction1(scalarType, scope, attrName)
        }
        
        return null
    }
    
    override fun exitWhere(ctx: QLParser.WhereContext?) {
        val logicExpr = ctx?.logic_expr() ?: return
        val condition = parseLogicExpr(logicExpr)
        if (condition != null) {
            // If it's a simple condition, add it directly
            // If it's complex (And/Or/Not), we need to handle it differently
            // For now, we'll store it as a single condition in the list
            // The translator will handle it properly
            conditions.add(condition)
        }
    }
    
    override fun exitOrdered_expression_root(ctx: QLParser.Ordered_expression_rootContext?) {
        val expr = ctx?.arith_expr() ?: return
        val direction = when {
            ctx.order_dir()?.ORDER_DESC() != null -> SortDirection.DESC
            else -> SortDirection.ASC
        }
        
        // Check if ORDER BY contains a function
        val func = expr.func()
        if (func != null) {
            val function = parseFunction(func)
            if (function != null) {
                // ORDER BY function (e.g., count(e:concept:name))
                val scope = when (function) {
                    is PqlFunction.AggregationFunction -> function.scope ?: PqlScope.EVENT
                    is PqlFunction.ScalarFunction1 -> function.scope ?: PqlScope.EVENT
                    is PqlFunction.ScalarFunction0 -> PqlScope.EVENT
                }
                // Store the full expression text as attribute for ORDER BY
                orderBy = PqlOrder(scope, expr.text, direction)
                return
            }
        }
        
        // Check if ORDER BY contains arithmetic expression
        val hasArithmetic = expr.children?.any { 
            it is org.antlr.v4.runtime.tree.TerminalNode && 
            (it.text == "+" || it.text == "-" || it.text == "*" || it.text == "/")
        } ?: false
        
        if (hasArithmetic) {
            // ORDER BY arithmetic expression - store full text
            val scope = extractScopeFromArithmetic(parseArithmeticExpression(expr) ?: return) ?: PqlScope.EVENT
            orderBy = PqlOrder(scope, expr.text, direction)
            return
        }
        
        // Regular attribute ORDER BY
        val attribute = parseAttribute(expr)
        if (attribute != null) {
            val scope = attribute.first
            val attrName = attribute.second
            detectedScope = scope
            orderBy = PqlOrder(scope, attrName, direction)
        }
    }
    
    override fun exitLimit_number(ctx: QLParser.Limit_numberContext?) {
        val number = ctx?.NUMBER()?.text ?: return
        try {
            // LIMIT can have multiple numbers, take the first one
            if (limit == null) {
                limit = number.toInt()
            }
        } catch (e: NumberFormatException) {
            // Ignore invalid numbers
        }
    }
    
    override fun exitOffset_number(ctx: QLParser.Offset_numberContext?) {
        val number = ctx?.NUMBER()?.text ?: return
        try {
            // OFFSET can have multiple numbers, take the first one
            if (offset == null) {
                offset = number.toInt()
            }
        } catch (e: NumberFormatException) {
            // Ignore invalid numbers
        }
    }
    
    override fun exitGroup_by(ctx: QLParser.Group_byContext?) {
        val idList = ctx?.id_list() ?: return
        val ids = idList.ID()
        
        ids.forEach { idToken ->
            val id = idToken.text
            val (scope, attribute) = parseScopeAndAttribute(id) ?: return@forEach
            detectedScope = scope
            groupByFields.add(PqlGroupByField(scope, attribute))
        }
    }
    
    private fun parseAttribute(ctx: QLParser.Arith_exprContext): Pair<PqlScope, String>? {
        // Simple case: just an ID (e:name, t:name, etc.)
        val id = ctx.ID()?.text ?: return null
        
        return parseScopeAndAttribute(id)
    }
    
    private fun parseScopeAndAttribute(id: String): Pair<PqlScope, String>? {
        // Parse scope prefix (e:, t:, l:)
        // Handle hoisting prefixes (^, ^^)
        val trimmed = id.trim()
        val hoistingPrefix = trimmed.takeWhile { it == '^' }
        val withoutHoisting = trimmed.drop(hoistingPrefix.length)
        
        val parts = withoutHoisting.split(":", limit = 2)
        if (parts.size != 2) return null
        
        val scopeToken = parts[0].lowercase()
        val attribute = parts[1]
        
        val scope = when (scopeToken) {
            "e", "event", "events" -> PqlScope.EVENT
            "t", "trace", "traces" -> PqlScope.TRACE
            "l", "log", "logs" -> PqlScope.LOG
            else -> return null
        }
        
        return Pair(scope, attribute)
    }
    
    /**
     * Parse a logic_expr into a PqlCondition tree
     */
    private fun parseLogicExpr(ctx: QLParser.Logic_exprContext): PqlCondition? {
        // Handle NOT first - it has highest precedence in unary operations
        val logicExprs = ctx.logic_expr()
        if (ctx.OP_NOT() != null && logicExprs.size == 1) {
            val inner = parseLogicExpr(logicExprs[0]) ?: return null
            return PqlCondition.Not(inner)
        }
        
        // Handle parentheses: (logic_expr)
        if (logicExprs.size == 1 && ctx.arith_expr().isEmpty() && ctx.OP_NOT() == null) {
            return parseLogicExpr(logicExprs[0])
        }
        
        // Handle logical operators: AND, OR
        if (logicExprs.size == 2) {
            val left = parseLogicExpr(logicExprs[0]) ?: return null
            val right = parseLogicExpr(logicExprs[1]) ?: return null
            
            return when {
                ctx.OP_AND() != null -> PqlCondition.And(listOf(left, right))
                ctx.OP_OR() != null -> PqlCondition.Or(listOf(left, right))
                else -> null
            }
        }
        
        // Handle comparison operators with two arith_expr
        val arithExprs = ctx.arith_expr()
        if (arithExprs.size == 2) {
            val left = arithExprs[0]
            val right = arithExprs[1]
            
            val leftAttr = parseAttribute(left) ?: return null
            val rightValue = extractValue(right)
            
            val operator = when {
                ctx.OP_EQ() != null -> PqlOperator.EQ
                ctx.OP_NEQ() != null -> PqlOperator.NEQ
                ctx.OP_LT() != null -> PqlOperator.LT
                ctx.OP_LE() != null -> PqlOperator.LE
                ctx.OP_GT() != null -> PqlOperator.GT
                ctx.OP_GE() != null -> PqlOperator.GE
                else -> return null
            }
            
            if (rightValue == null) return null
            
            val scope = leftAttr.first
            detectedScope = scope
            
            return PqlCondition.Simple(
                scope = scope,
                attribute = leftAttr.second,
                operator = operator,
                value = rightValue
            )
        }
        
        // Handle IN / NOT IN
        if (arithExprs.size == 1 && ctx.in_list() != null) {
            val left = arithExprs[0]
            val leftAttr = parseAttribute(left) ?: return null
            val inList = parseInList(ctx.in_list()!!) ?: return null
            
            val operator = when {
                ctx.OP_IN() != null -> PqlOperator.IN
                ctx.OP_NOT_IN() != null -> PqlOperator.NOT_IN
                else -> return null
            }
            
            val scope = leftAttr.first
            detectedScope = scope
            
            return PqlCondition.Simple(
                scope = scope,
                attribute = leftAttr.second,
                operator = operator,
                value = inList
            )
        }
        
        // Handle LIKE / MATCHES
        if (arithExprs.size == 1 && ctx.STRING() != null) {
            val left = arithExprs[0]
            val leftAttr = parseAttribute(left) ?: return null
            val pattern = ctx.STRING()!!.text.trim('"', '\'')
            
            val operator = when {
                ctx.OP_LIKE() != null -> PqlOperator.LIKE
                ctx.OP_MATCHES() != null -> PqlOperator.MATCHES
                else -> return null
            }
            
            val scope = leftAttr.first
            detectedScope = scope
            
            return PqlCondition.Simple(
                scope = scope,
                attribute = leftAttr.second,
                operator = operator,
                value = pattern
            )
        }
        
        // Handle IS NULL / IS NOT NULL
        if (arithExprs.size == 1) {
            val left = arithExprs[0]
            val leftAttr = parseAttribute(left) ?: return null
            
            val operator = when {
                ctx.OP_IS_NULL() != null -> PqlOperator.IS_NULL
                ctx.OP_IS_NOT_NULL() != null -> PqlOperator.IS_NOT_NULL
                else -> return null
            }
            
            val scope = leftAttr.first
            detectedScope = scope
            
            return PqlCondition.Simple(
                scope = scope,
                attribute = leftAttr.second,
                operator = operator,
                value = null
            )
        }
        
        return null
    }
    
    private fun parseInList(ctx: QLParser.In_listContext): List<String>? {
        val idOrScalarList = ctx.id_or_scalar_list() ?: return null
        val items = mutableListOf<String>()
        
        // Parse ID or scalar items
        val ids = idOrScalarList.ID()
        val scalars = idOrScalarList.scalar()
        
        ids.forEach { item ->
            items.add(item.text)
        }
        
        scalars.forEach { scalar ->
            val value = when {
                scalar.STRING() != null -> scalar.STRING()!!.text.trim('"', '\'')
                scalar.NUMBER() != null -> scalar.NUMBER()!!.text
                scalar.BOOLEAN() != null -> scalar.BOOLEAN()!!.text
                scalar.NULL() != null -> "null"
                else -> return null
            }
            items.add(value)
        }
        
        return items.ifEmpty { null }
    }
    
    private fun extractValue(ctx: QLParser.Arith_exprContext): String? {
        // Extract string, number, or ID value
        // Check scalar first
        val scalar = ctx.scalar()
        if (scalar != null) {
            return when {
                scalar.STRING() != null -> scalar.STRING()!!.text.trim('"', '\'')
                scalar.NUMBER() != null -> scalar.NUMBER()!!.text
                scalar.BOOLEAN() != null -> scalar.BOOLEAN()!!.text
                scalar.NULL() != null -> "null"
                else -> null
            }
        }
        
        // Check ID
        return ctx.ID()?.text
    }
    
    override fun enterDelete_query(ctx: QLParser.Delete_queryContext?) {
        isDeleteQuery = true
    }
    
    override fun exitDelete(ctx: QLParser.DeleteContext?) {
        val scopeToken = ctx?.SCOPE()?.text
        if (scopeToken != null) {
            deleteScope = PqlScope.fromToken(scopeToken)
        }
    }
    
    fun buildQuery(): PqlQuery {
        // Determine collection scope from attributes
        // Scope is determined by the prefixes in attributes (e:, t:, l:)
        // If no scope detected, default to EVENT
        
        // Validate that we have at least some projections or selectAll
        if (!selectAll && projections.isEmpty()) {
            throw IllegalArgumentException("Query must have at least one projection or use SELECT *")
        }
        
        val finalScope = detectedScope ?: PqlScope.EVENT
        
        return PqlQuery(
            collection = finalScope,
            projections = projections,
            conditions = conditions,
            orderBy = orderBy,
            limit = limit,
            offset = offset,
            groupBy = groupByFields,
            selectAll = selectAll
        )
    }
    
    fun buildDeleteQuery(): PqlDeleteQuery {
        return PqlDeleteQuery(
            scope = deleteScope,
            conditions = conditions,
            orderBy = orderBy,
            limit = limit,
            offset = offset
        )
    }
    
    fun isDelete(): Boolean = isDeleteQuery
}


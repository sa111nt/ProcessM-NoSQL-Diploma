package pql.parser

import pql.model.*
import ql.QLParser
import ql.QLParserBaseListener

class PqlQueryListener : QLParserBaseListener() {

    private var selectAll: Boolean = false
    private val projections = mutableListOf<PqlProjection>()
    private val conditions = mutableListOf<PqlCondition>()
    private val orderByList = mutableListOf<PqlOrder>()

    private var limit: Int? = null
    private var offset: Int? = null
    private val limitsMap = mutableMapOf<PqlScope, Int>()
    private val offsetsMap = mutableMapOf<PqlScope, Int>()

    private val groupByFields = mutableListOf<PqlGroupByField>()
    private var detectedScope: PqlScope? = null
    private var isDeleteQuery: Boolean = false
    private var deleteScope: PqlScope? = null

    private fun updateDetectedScope(newScope: PqlScope?) {
        if (newScope == null) return
        val currentLevel = detectedScope?.let { getScopeHierarchyLevel(it) } ?: 0
        val newLevel = getScopeHierarchyLevel(newScope)
        if (newLevel > currentLevel) {
            detectedScope = newScope
        }
    }

    private fun getScopeHierarchyLevel(scope: PqlScope): Int = when (scope) {
        PqlScope.LOG -> 1
        PqlScope.TRACE -> 2
        PqlScope.EVENT -> 3
    }

    override fun enterSelect_all_implicit(ctx: QLParser.Select_all_implicitContext?) { selectAll = true }
    override fun enterSelect_all(ctx: QLParser.Select_allContext?) { selectAll = true }

    override fun enterScoped_select_all(ctx: QLParser.Scoped_select_allContext?) {
        val scopeToken = ctx?.SCOPE()?.text
        if (scopeToken != null) {
            val scope = PqlScope.fromToken(scopeToken)
            updateDetectedScope(scope)
            selectAll = true
        }
    }

    override fun exitColumn_list_arith_expr_root(ctx: QLParser.Column_list_arith_expr_rootContext?) {
        val expr = ctx?.arith_expr_root() ?: return
        val arithExpr = expr.arith_expr()

        val func = arithExpr.func()
        if (func != null) {
            val function = parseFunction(func)
            if (function != null) {
                val funcScope = when (function) {
                    is PqlFunction.AggregationFunction -> function.scope
                    is PqlFunction.ScalarFunction1 -> extractScopeFromArithmetic(function.expression)
                    is PqlFunction.ScalarFunction0 -> null
                }
                updateDetectedScope(funcScope)
                projections.add(
                    PqlProjection(
                        scope = funcScope,
                        attribute = when (function) {
                            is PqlFunction.AggregationFunction -> function.attribute
                            is PqlFunction.ScalarFunction1 -> null
                            is PqlFunction.ScalarFunction0 -> null
                        },
                        raw = expr.text,
                        function = function
                    )
                )
                return
            }
        }

        val arithmeticExpr = parseArithmeticExpression(arithExpr)
        if (arithmeticExpr != null && arithmeticExpr !is PqlArithmeticExpression.Attribute && arithmeticExpr !is PqlArithmeticExpression.Literal) {
            val scope = extractScopeFromArithmetic(arithmeticExpr)
            updateDetectedScope(scope)
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

        val attribute = parseAttribute(arithExpr)
        if (attribute != null) {
            val scope = attribute.first
            val attrName = attribute.second
            updateDetectedScope(scope)
            projections.add(
                PqlProjection(
                    scope = scope,
                    attribute = attrName,
                    raw = expr.text
                )
            )
        } else {
            projections.add(PqlProjection(scope = detectedScope ?: PqlScope.EVENT, attribute = null, raw = expr.text))
        }
    }

    private fun parseArithmeticExpression(ctx: QLParser.Arith_exprContext): PqlArithmeticExpression? {
        val children = ctx.children ?: return null

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

        if (ctx.arith_expr().size == 1 && ctx.text.startsWith("(") && ctx.text.endsWith(")")) {
            return parseArithmeticExpression(ctx.arith_expr(0))
        }

        val func = ctx.func()
        if (func != null) {
            val function = parseFunction(func) ?: return null
            return PqlArithmeticExpression.FunctionCall(function)
        }

        val id = ctx.ID()
        if (id != null) {
            val (scope, attribute) = parseScopeAndAttribute(ctx.text) ?: return null
            return PqlArithmeticExpression.Attribute(scope, attribute)
        }

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
                    is PqlFunction.ScalarFunction1 -> extractScopeFromArithmetic(func.expression)
                    is PqlFunction.ScalarFunction0 -> null
                }
            }
            is PqlArithmeticExpression.Literal -> null
        }
    }

    private fun parseFunction(ctx: QLParser.FuncContext): PqlFunction? {
        val funcAggr = ctx.FUNC_AGGR()
        if (funcAggr != null) {
            val innerText = ctx.text.substringAfter("(").substringBeforeLast(")")
            val (scope, attribute) = parseScopeAndAttribute(innerText) ?: return null

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

        val funcScalar0 = ctx.FUNC_SCALAR0()
        if (funcScalar0 != null) {
            val funcName = funcScalar0.text.lowercase()
            val scalarType = when {
                funcName.contains("now") -> PqlFunction.Scalar0.NOW
                else -> return null
            }
            return PqlFunction.ScalarFunction0(scalarType)
        }

        val funcScalar1 = ctx.FUNC_SCALAR1()
        if (funcScalar1 != null) {
            val arithExprCtx = ctx.arith_expr() ?: return null
            val expr = parseArithmeticExpression(arithExprCtx) ?: return null

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

            return PqlFunction.ScalarFunction1(scalarType, expr)
        }
        return null
    }

    override fun exitWhere(ctx: QLParser.WhereContext?) {
        val logicExpr = ctx?.logic_expr() ?: return
        val condition = parseLogicExpr(logicExpr)
        if (condition != null) {
            conditions.add(condition)
        }
    }

    override fun exitOrdered_expression_root(ctx: QLParser.Ordered_expression_rootContext?) {
        val expr = ctx?.arith_expr() ?: return

        val hasDescToken = ctx.order_dir()?.ORDER_DESC() != null
        val direction = if (hasDescToken) SortDirection.DESC else SortDirection.ASC

        val func = expr.func()
        if (func != null) {
            val function = parseFunction(func)
            if (function != null) {
                val scope = when (function) {
                    is PqlFunction.AggregationFunction -> function.scope ?: PqlScope.EVENT
                    is PqlFunction.ScalarFunction1 -> extractScopeFromArithmetic(function.expression) ?: PqlScope.EVENT
                    is PqlFunction.ScalarFunction0 -> PqlScope.EVENT
                }
                updateDetectedScope(scope)
                orderByList.add(PqlOrder(scope, expr.text, direction, function = function))
                return
            }
        }

        val arithmeticExpr = parseArithmeticExpression(expr)
        if (arithmeticExpr != null && arithmeticExpr !is PqlArithmeticExpression.Attribute && arithmeticExpr !is PqlArithmeticExpression.Literal) {
            val scope = extractScopeFromArithmetic(arithmeticExpr) ?: PqlScope.EVENT
            updateDetectedScope(scope)
            orderByList.add(PqlOrder(scope, expr.text, direction, arithmeticExpression = arithmeticExpr))
            return
        }

        val attribute = parseAttribute(expr)
        if (attribute != null) {
            val scope = attribute.first
            val attrName = attribute.second
            updateDetectedScope(scope)
            orderByList.add(PqlOrder(scope, attrName, direction))
        } else {
            orderByList.add(PqlOrder(detectedScope ?: PqlScope.EVENT, expr.text, direction))
        }
    }

    override fun exitLimit_number(ctx: QLParser.Limit_numberContext?) {
        val text = ctx?.text ?: return
        val parts = text.split(":")
        try {
            if (parts.size == 2) {
                limitsMap[PqlScope.fromToken(parts[0])] = parts[1].toInt()
            } else {
                val number = text.toInt()
                if (limit == null) limit = number
            }
        } catch (e: Exception) {}
    }

    override fun exitOffset_number(ctx: QLParser.Offset_numberContext?) {
        val text = ctx?.text ?: return
        val parts = text.split(":")
        try {
            if (parts.size == 2) {
                offsetsMap[PqlScope.fromToken(parts[0])] = parts[1].toInt()
            } else {
                val number = text.toInt()
                if (offset == null) offset = number
            }
        } catch (e: Exception) {}
    }

    override fun exitGroup_by(ctx: QLParser.Group_byContext?) {
        val idList = ctx?.id_list() ?: return
        val ids = idList.ID()

        ids.forEach { idToken ->
            val id = idToken.text
            val isLiteral = ctx.text.contains("[$id]")
            val idToPass = if (isLiteral) "[$id]" else id

            val (scope, attribute) = parseScopeAndAttribute(idToPass) ?: return@forEach
            updateDetectedScope(scope)
            groupByFields.add(PqlGroupByField(scope, attribute))
        }
    }

    private fun parseAttribute(ctx: QLParser.Arith_exprContext): Pair<PqlScope, String>? {
        if (ctx.ID() == null) return null
        return parseScopeAndAttribute(ctx.text)
    }

    private fun parseScopeAndAttribute(id: String): Pair<PqlScope, String>? {
        val cleanId = id.trim()
        val isLiteral = cleanId.startsWith("[") && cleanId.endsWith("]")

        val trimmed = cleanId.removePrefix("[").removeSuffix("]")

        val hoistingPrefix = trimmed.takeWhile { it == '^' }
        val withoutHoisting = trimmed.drop(hoistingPrefix.length)

        val parts = withoutHoisting.split(":", limit = 2)

        if (parts.size == 1) {
            val scope = detectedScope ?: PqlScope.EVENT
            val attr = hoistingPrefix + withoutHoisting
            return Pair(scope, if (isLiteral) "[$attr]" else attr)
        }

        val possibleScopeToken = parts[0].lowercase()
        val parsedScope = when (possibleScopeToken) {
            "e", "event", "events" -> PqlScope.EVENT
            "t", "trace", "traces" -> PqlScope.TRACE
            "l", "log", "logs" -> PqlScope.LOG
            "c", "classifier" -> PqlScope.EVENT
            else -> null
        }

        if (parsedScope != null) {
            val attr = if (possibleScopeToken == "c" || possibleScopeToken == "classifier") {
                hoistingPrefix + withoutHoisting
            } else {
                hoistingPrefix + parts[1]
            }
            return Pair(parsedScope, if (isLiteral) "[$attr]" else attr)
        } else {
            val scope = detectedScope ?: PqlScope.EVENT
            val attr = hoistingPrefix + withoutHoisting
            return Pair(scope, if (isLiteral) "[$attr]" else attr)
        }
    }

    private fun parseLogicExpr(ctx: QLParser.Logic_exprContext): PqlCondition? {
        val logicExprs = ctx.logic_expr()
        if (ctx.OP_NOT() != null && logicExprs.size == 1) {
            val inner = parseLogicExpr(logicExprs[0]) ?: return null
            return PqlCondition.Not(inner)
        }

        if (logicExprs.size == 1 && ctx.arith_expr().isEmpty() && ctx.OP_NOT() == null) {
            return parseLogicExpr(logicExprs[0])
        }

        if (logicExprs.size == 2) {
            val left = parseLogicExpr(logicExprs[0]) ?: return null
            val right = parseLogicExpr(logicExprs[1]) ?: return null

            return when {
                ctx.OP_AND() != null -> PqlCondition.And(listOf(left, right))
                ctx.OP_OR() != null -> PqlCondition.Or(listOf(left, right))
                else -> null
            }
        }

        val arithExprs = ctx.arith_expr()

        if (arithExprs.size == 1 && ctx.children.size == 1) {
            val value = extractValue(arithExprs[0])
            if (value is Boolean) {
                if (value) return null else return PqlCondition.AlwaysFalse
            }
            if (value?.toString()?.lowercase() == "true") return null
            if (value?.toString()?.lowercase() == "false") return PqlCondition.AlwaysFalse
            return null
        }

        if (arithExprs.size == 2) {
            val left = arithExprs[0]
            val right = arithExprs[1]

            val leftAttr = parseAttribute(left)
            if (leftAttr == null) {
                val leftVal = extractValue(left)
                val rightVal = extractValue(right)
                if (leftVal != null && rightVal != null) {
                    val isEqual = leftVal == rightVal
                    val operator = when {
                        ctx.OP_EQ() != null -> isEqual
                        ctx.OP_NEQ() != null -> !isEqual
                        else -> false
                    }
                    return if (!operator) PqlCondition.AlwaysFalse else null
                }
                return null
            }

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
            updateDetectedScope(scope)

            return PqlCondition.Simple(scope = scope, attribute = leftAttr.second, operator = operator, value = rightValue)
        }

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
            updateDetectedScope(scope)
            return PqlCondition.Simple(scope = scope, attribute = leftAttr.second, operator = operator, value = inList)
        }

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
            updateDetectedScope(scope)
            return PqlCondition.Simple(scope = scope, attribute = leftAttr.second, operator = operator, value = pattern)
        }

        if (arithExprs.size == 1) {
            val left = arithExprs[0]
            val leftAttr = parseAttribute(left) ?: return null

            val operator = when {
                ctx.OP_IS_NULL() != null -> PqlOperator.IS_NULL
                ctx.OP_IS_NOT_NULL() != null -> PqlOperator.IS_NOT_NULL
                else -> return null
            }

            val scope = leftAttr.first
            updateDetectedScope(scope)
            return PqlCondition.Simple(scope = scope, attribute = leftAttr.second, operator = operator, value = null)
        }

        return null
    }

    private fun parseInList(ctx: QLParser.In_listContext): List<String>? {
        val idOrScalarList = ctx.id_or_scalar_list() ?: return null
        val items = mutableListOf<String>()

        val ids = idOrScalarList.ID()
        val scalars = idOrScalarList.scalar()

        ids.forEach { items.add(it.text) }
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

    private fun extractValue(ctx: QLParser.Arith_exprContext): Any? {
        val scalar = ctx.scalar()
        if (scalar != null) {
            return when {
                scalar.STRING() != null -> scalar.STRING()!!.text.trim('"', '\'')
                scalar.NUMBER() != null -> {
                    val txt = scalar.NUMBER()!!.text
                    txt.toDoubleOrNull() ?: txt
                }
                scalar.BOOLEAN() != null -> {
                    val txt = scalar.BOOLEAN()!!.text.lowercase()
                    val boolStr = if (txt.contains(":")) txt.substringAfter(":") else txt
                    boolStr.toBoolean()
                }
                scalar.DATETIME() != null -> {
                    val txt = scalar.DATETIME()!!.text
                    val dateStr = if (txt.contains(":")) txt.substringAfter(":") else txt
                    if (dateStr.startsWith("D")) dateStr.substring(1) else dateStr
                }
                scalar.NULL() != null -> null
                else -> null
            }
        }

        val text = ctx.ID()?.text
        if (text != null) {
            val parts = text.split(":", limit = 2)
            val rawValue = if (parts.size == 2 && parts[0].lowercase() in listOf("e", "event", "t", "trace", "l", "log")) {
                parts[1]
            } else {
                text
            }
            return rawValue.toDoubleOrNull() ?: rawValue
        }
        return null
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
        val isSelectAll = selectAll || projections.isEmpty()

        val finalScope = if (projections.isEmpty() && groupByFields.isEmpty()) {
            PqlScope.EVENT
        } else {
            val hasEvent = projections.any { it.scope == PqlScope.EVENT || it.allAttributes } ||
                    groupByFields.any { it.scope == PqlScope.EVENT } ||
                    orderByList.any { it.scope == PqlScope.EVENT }
            val hasTrace = projections.any { it.scope == PqlScope.TRACE } ||
                    groupByFields.any { it.scope == PqlScope.TRACE } ||
                    orderByList.any { it.scope == PqlScope.TRACE }
            when {
                hasEvent || isSelectAll -> PqlScope.EVENT
                hasTrace -> PqlScope.TRACE
                else -> PqlScope.LOG
            }
        }

        return PqlQuery(
            collection = finalScope,
            projections = projections,
            conditions = conditions,
            orderBy = orderByList,
            limit = limit,
            offset = offset,
            limits = limitsMap,
            offsets = offsetsMap,
            groupBy = groupByFields,
            selectAll = isSelectAll
        )
    }

    fun buildDeleteQuery(): PqlDeleteQuery {
        return PqlDeleteQuery(
            scope = deleteScope,
            conditions = conditions,
            orderBy = orderByList,
            limit = limit,
            offset = offset
        )
    }

    fun isDelete(): Boolean = isDeleteQuery
}
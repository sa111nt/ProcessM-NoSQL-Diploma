package pql

import java.util.Locale

class PqlParser {

    private val selectRegex = Regex("\\bselect\\b", RegexOption.IGNORE_CASE)
    private val fromRegex = Regex("\\bfrom\\b", RegexOption.IGNORE_CASE)
    private val whereRegex = Regex("\\bwhere\\b", RegexOption.IGNORE_CASE)
    private val orderRegex = Regex("\\border\\s+by\\b", RegexOption.IGNORE_CASE)
    private val limitRegex = Regex("\\blimit\\b", RegexOption.IGNORE_CASE)
    private val andRegex = Regex("\\s+and\\s+", RegexOption.IGNORE_CASE)
    private val aliasRegex = Regex("\\s+as\\s+", RegexOption.IGNORE_CASE)

    fun parse(input: String): PqlQuery {
        val normalized = input.trim()
        require(normalized.isNotEmpty()) { "PQL query cannot be empty" }

        val cleaned = normalized.trimEnd(';').trim()
        val selectMatch = selectRegex.find(cleaned)
            ?: error("Query must start with select")
        require(selectMatch.range.first == 0) { "Query must start with select" }

        val fromMatch = fromRegex.find(cleaned, selectMatch.range.last + 1)
            ?: error("Missing from clause")

        val whereMatch = whereRegex.find(cleaned, fromMatch.range.last + 1)
        val orderMatch = orderRegex.find(cleaned, fromMatch.range.last + 1)
        val limitMatch = limitRegex.find(cleaned, fromMatch.range.last + 1)

        val fieldsPart = cleaned.substring(selectMatch.range.last + 1, fromMatch.range.first).trim()
        require(fieldsPart.isNotEmpty()) { "select clause cannot be empty" }

        val fromSectionEnd = listOfNotNull(whereMatch, orderMatch, limitMatch)
            .minByOrNull { it.range.first }?.range?.first ?: cleaned.length
        val collectionPart = cleaned.substring(fromMatch.range.last + 1, fromSectionEnd).trim()
        require(collectionPart.isNotEmpty()) { "from clause cannot be empty" }
        val collectionScope = PqlScope.fromToken(collectionPart)

        val whereSection = whereMatch?.let {
            val end = listOfNotNull(orderMatch, limitMatch)
                .filter { match -> match.range.first > it.range.first }
                .minByOrNull { match -> match.range.first }?.range?.first ?: cleaned.length
            cleaned.substring(it.range.last + 1, end).trim()
        }

        val orderSection = orderMatch?.let {
            val end = limitMatch?.takeIf { limit -> limit.range.first > it.range.first }?.range?.first ?: cleaned.length
            cleaned.substring(it.range.last + 1, end).trim()
        }

        val limitSection = limitMatch?.let {
            cleaned.substring(it.range.last + 1).trim()
        }

        val (projections, selectAll) = parseProjections(fieldsPart, collectionScope)
        val conditions = whereSection?.let { parseConditions(it, collectionScope) } ?: emptyList()
        val order = orderSection?.takeIf { it.isNotEmpty() }?.let { parseOrder(it, collectionScope) }
        val limit = limitSection?.takeIf { it.isNotEmpty() }?.let { parseLimit(it) }

        return PqlQuery(
            collection = collectionScope,
            projections = projections,
            conditions = conditions,
            orderBy = order,
            limit = limit,
            selectAll = selectAll
        )
    }

    private fun parseProjections(section: String, defaultScope: PqlScope): Pair<List<PqlProjection>, Boolean> {
        val tokens = splitCommaSeparated(section)
        require(tokens.isNotEmpty()) { "select clause must contain at least one projection" }

        var selectAll = false
        val projections = tokens.map { token ->
            val trimmed = token.trim()
            if (trimmed == "*") {
                selectAll = true
                PqlProjection(scope = null, attribute = null, alias = null, allAttributes = true, raw = trimmed)
            } else {
                val parts = aliasRegex.split(trimmed, limit = 2)
                val expr = parts[0].trim()
                val alias = parts.getOrNull(1)?.trim()?.takeIf { it.isNotEmpty() }

                val (scope, attribute, all) = parseScopedAttribute(expr, defaultScope)
                if (all) selectAll = true
                PqlProjection(scope, attribute, alias, all, trimmed)
            }
        }

        return projections to selectAll
    }

    private fun parseConditions(section: String, defaultScope: PqlScope): List<PqlCondition> {
        val fragments = andRegex.split(section).map { it.trim() }.filter { it.isNotEmpty() }
        return fragments.map { cond ->
            val operator: PqlOperator
            val parts: List<String>
            when {
                cond.contains("!=") -> {
                    operator = PqlOperator.NEQ
                    parts = cond.split("!=", limit = 2)
                }
                cond.contains("=") -> {
                    operator = PqlOperator.EQ
                    parts = cond.split("=", limit = 2)
                }
                else -> error("Unsupported operator in condition: $cond")
            }
            require(parts.size == 2) { "Invalid condition format: $cond" }
            val (scope, attribute) = parseScopedAttribute(parts[0].trim(), defaultScope).let {
                require(!it.third) { "Wildcard is not allowed in where clause: ${parts[0]}" }
                it.first!! to it.second!!
            }
            val value = parseValue(parts[1].trim())
            PqlCondition(scope, attribute, operator, value)
        }
    }

    private fun parseOrder(section: String, defaultScope: PqlScope): PqlOrder {
        val parts = section.trim().split(Regex("\\s+"))
        require(parts.isNotEmpty()) { "order by clause cannot be empty" }
        val expr = parts[0]
        val direction = if (parts.size > 1) {
            when (parts[1].lowercase(Locale.getDefault())) {
                "asc" -> SortDirection.ASC
                "desc" -> SortDirection.DESC
                else -> error("Invalid sort direction: ${parts[1]}")
            }
        } else SortDirection.ASC
        val (scope, attribute) = parseScopedAttribute(expr, defaultScope).let {
            require(!it.third) { "Wildcard is not allowed in order by clause: $expr" }
            it.first!! to it.second!!
        }
        return PqlOrder(scope, attribute, direction)
    }

    private fun parseLimit(section: String): Int {
        val raw = section.trim()
        require(raw.isNotEmpty()) { "limit value cannot be empty" }
        return raw.toIntOrNull()?.takeIf { it > 0 }
            ?: error("limit must be a positive integer: $raw")
    }

    private fun parseScopedAttribute(
        expression: String,
        defaultScope: PqlScope
    ): Triple<PqlScope?, String?, Boolean> {
        if (expression == "*") {
            return Triple(null, null, true)
        }
        val separatorIndex = expression.indexOf(':')
        val scope: PqlScope
        val attribute: String
        if (separatorIndex == -1) {
            scope = defaultScope
            attribute = expression.trim()
        } else {
            scope = PqlScope.fromToken(expression.substring(0, separatorIndex))
            attribute = expression.substring(separatorIndex + 1).trim()
        }
        require(attribute.isNotEmpty()) { "Attribute name cannot be empty in expression: $expression" }
        val wildcard = attribute == "*"
        return Triple(scope, if (wildcard) null else attribute, wildcard)
    }

    private fun parseValue(raw: String): String {
        val trimmed = raw.trim()
        if ((trimmed.startsWith("'") && trimmed.endsWith("'")) ||
            (trimmed.startsWith("\"") && trimmed.endsWith("\""))
        ) {
            return trimmed.substring(1, trimmed.length - 1)
        }
        return trimmed
    }

    private fun splitCommaSeparated(section: String): List<String> {
        return section.split(',')
            .map { it.trim() }
            .filter { it.isNotEmpty() }
    }
}


package app

import com.google.gson.GsonBuilder
import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonNull
import com.google.gson.JsonObject
import db.CouchDBManager
import pql.*
import java.io.BufferedReader
import java.io.InputStreamReader

class PqlInterpreter(
    private val couchDB: CouchDBManager,
    private val databaseName: String = "event_logs"
) {

    private val parser = AntlrPqlParser()
    private val translator = PqlToCouchDbTranslator()
    private val gson = GsonBuilder().setPrettyPrinting().create()

    fun start() {
        println("💬 Enter PQL queries (finish with ';', type 'exit' to quit)")
        val reader = BufferedReader(InputStreamReader(System.`in`))
        val buffer = StringBuilder()

        while (true) {
            val prompt = if (buffer.isEmpty()) "PQL> " else "... "
            print(prompt)
            val line = reader.readLine() ?: break
            val trimmed = line.trim()

            if (buffer.isEmpty() && trimmed.equals("exit", ignoreCase = true)) {
                println("👋 Exiting PQL interpreter.")
                break
            }
            if (trimmed.equals("exit", ignoreCase = true)) {
                println("⚠️ Pending query discarded.")
                buffer.clear()
                continue
            }

            buffer.appendLine(line)
            if (trimmed.endsWith(";")) {
                val statement = buffer.toString().trim()
                buffer.clear()
                val queryText = statement.removeSuffix(";").trim()
                if (queryText.isNotEmpty()) {
                    processQuery(queryText)
                }
            }
        }
    }

    private fun processQuery(queryText: String) {
        try {
            // Check if it's a DELETE query
            if (queryText.trim().lowercase().startsWith("delete")) {
                processDeleteQuery(queryText)
            } else {
                processSelectQuery(queryText)
            }
        } catch (e: Exception) {
            println("❌ ${e.message}")
        }
    }
    
    /**
     * Executes a PQL SELECT query and returns the result as a JsonElement.
     * This method is public so it can be used in tests.
     */
    fun executeQuery(queryText: String): JsonElement {
        val pqlQuery = parser.parse(queryText)

        // Check for cross-scope WHERE conditions
        val crossScopeConditions = pqlQuery.conditions.filter { getCrossScope(it, pqlQuery.collection) != null }
        val sameScopeConditions = pqlQuery.conditions.filter { getCrossScope(it, pqlQuery.collection) == null }

        var resolvedQuery = pqlQuery

        if (crossScopeConditions.isNotEmpty()) {
            // Resolve cross-scope conditions: query outer scope first, then filter by logId
            val logIds = resolveCrossScopeConditions(crossScopeConditions, pqlQuery.collection)

            if (logIds.isEmpty()) {
                // No matching outer docs → return empty result
                return JsonArray()
            }

            // Add logId filter to the same-scope conditions
            val logIdCondition = if (logIds.size == 1) {
                PqlCondition.Simple(pqlQuery.collection, "logId", PqlOperator.EQ, logIds.first())
            } else {
                PqlCondition.Simple(pqlQuery.collection, "logId", PqlOperator.IN, logIds)
            }

            resolvedQuery = pqlQuery.copy(
                conditions = sameScopeConditions + logIdCondition
            )
        }

        // Check if ORDER BY spans multiple scopes (e.g., t: and e:)
        val orderByScopes = resolvedQuery.orderBy.map { it.scope }.distinct()
        val isMultiScopeOrderBy = orderByScopes.size > 1

        if (isMultiScopeOrderBy) {
            return executeMultiScopeQuery(resolvedQuery)
        }

        // Single-scope path (original behavior)
        // Only create index for ORDER BY if not using GROUP BY
        // CouchDB requires a single composite index covering ALL sort fields
        if (resolvedQuery.groupBy.isEmpty() && resolvedQuery.orderBy.isNotEmpty()) {
            val sortFields = mutableListOf("docType")
            for (order in resolvedQuery.orderBy) {
                val sortField = translator.resolveField(order.scope, order.attribute).toDotPath()
                sortFields.add(sortField)
            }
            couchDB.ensureIndex(databaseName, sortFields)
        }
        val couchQuery = translator.translate(resolvedQuery)
        val response = couchDB.findDocs(databaseName, couchQuery)
        return formatResponse(response, resolvedQuery)
    }

    /**
     * Executes a query with multi-scope ORDER BY.
     * Groups events by trace, sorts at each level, applies limit as trace-level limit.
     */
    private fun executeMultiScopeQuery(query: PqlQuery): JsonElement {
        // 1. Fetch all events (without ORDER BY and LIMIT, we'll sort/limit ourselves)
        val eventQuery = query.copy(
            orderBy = emptyList(),
            limits = emptyList(),
            offsets = emptyList()
        )
        val couchQuery = translator.translate(eventQuery)
        // Override CouchDB's default limit (25) to fetch ALL events for grouping/sorting
        couchQuery.addProperty("limit", 100000)
        val allEvents = couchDB.findDocs(databaseName, couchQuery)

        if (allEvents.size() == 0) return formatResponse(JsonArray(), query)

        // 2. Group events by traceId
        val eventsByTrace = mutableMapOf<String, MutableList<JsonObject>>()
        for (element in allEvents) {
            if (!element.isJsonObject) continue
            val event = element.asJsonObject
            val traceId = event.get("traceId")?.asString ?: "unknown"
            eventsByTrace.getOrPut(traceId) { mutableListOf() }.add(event)
        }

        // 3. Sort events within each trace group (event-level ORDER BY)
        val eventOrderBy = query.orderBy.filter { it.scope == PqlScope.EVENT }
        if (eventOrderBy.isNotEmpty()) {
            for ((_, events) in eventsByTrace) {
                events.sortWith(buildComparator(eventOrderBy))
            }
        }

        // 4. Fetch trace docs if we need trace-level sorting
        val traceOrderBy = query.orderBy.filter { it.scope == PqlScope.TRACE }
        val traceDocsMap = mutableMapOf<String, JsonObject>()
        if (traceOrderBy.isNotEmpty()) {
            val traceSelector = JsonObject().apply {
                addProperty("docType", "trace")
            }
            val traceQuery = JsonObject().apply {
                add("selector", traceSelector)
                addProperty("limit", 10000)
            }
            val traceDocs = couchDB.findDocs(databaseName, traceQuery)
            for (doc in traceDocs) {
                if (doc.isJsonObject) {
                    val traceDoc = doc.asJsonObject
                    val id = traceDoc.get("_id")?.asString ?: continue
                    traceDocsMap[id] = traceDoc
                }
            }
        }

        // 5. Sort trace groups by trace-level ORDER BY
        var sortedTraceIds = eventsByTrace.keys.toList()
        if (traceOrderBy.isNotEmpty() && traceDocsMap.isNotEmpty()) {
            sortedTraceIds = sortedTraceIds.sortedWith(Comparator { id1, id2 ->
                val doc1 = traceDocsMap[id1]
                val doc2 = traceDocsMap[id2]
                if (doc1 == null || doc2 == null) return@Comparator 0
                compareByOrderFields(doc1, doc2, traceOrderBy)
            })
        }

        // 6. Apply LIMIT as trace-level limit when multi-scope ORDER BY is active
        val traceLimit = query.limits.firstOrNull()?.value
        if (traceLimit != null && sortedTraceIds.size > traceLimit) {
            sortedTraceIds = sortedTraceIds.take(traceLimit)
        }

        // 7. Flatten back to ordered event list and format
        val orderedEvents = JsonArray()
        for (traceId in sortedTraceIds) {
            val events = eventsByTrace[traceId] ?: continue
            for (event in events) {
                orderedEvents.add(event)
            }
        }

        return formatResponse(orderedEvents, query)
    }

    // Builds a Comparator for sorting JsonObjects by ORDER BY fields
    private fun buildComparator(orderByFields: List<PqlOrder>): Comparator<JsonObject> {
        return Comparator { a, b -> compareByOrderFields(a, b, orderByFields) }
    }

    // Compares two JsonObjects by a list of ORDER BY fields
    private fun compareByOrderFields(a: JsonObject, b: JsonObject, orderByFields: List<PqlOrder>): Int {
        for (order in orderByFields) {
            val fieldPath = translator.resolveField(order.scope, order.attribute)
            val valA = extractValue(a, fieldPath.segments)
            val valB = extractValue(b, fieldPath.segments)

            val cmp = compareJsonValues(valA, valB)
            if (cmp != 0) {
                return if (order.direction == SortDirection.DESC) -cmp else cmp
            }
        }
        return 0
    }

    // Compares two nullable JsonElements
    private fun compareJsonValues(a: JsonElement?, b: JsonElement?): Int {
        if (a == null && b == null) return 0
        if (a == null) return -1
        if (b == null) return 1
        if (a.isJsonNull && b.isJsonNull) return 0
        if (a.isJsonNull) return -1
        if (b.isJsonNull) return 1

        return try {
            val numA = a.asDouble
            val numB = b.asDouble
            numA.compareTo(numB)
        } catch (e: Exception) {
            a.asString.compareTo(b.asString)
        }
    }

    // Detects if a condition targets a different scope than the collection
    private fun getCrossScope(condition: PqlCondition, collectionScope: PqlScope): PqlScope? {
        return when (condition) {
            is PqlCondition.Simple -> if (condition.scope != collectionScope) condition.scope else null
            is PqlCondition.And -> condition.conditions.firstNotNullOfOrNull { getCrossScope(it, collectionScope) }
            is PqlCondition.Or -> condition.conditions.firstNotNullOfOrNull { getCrossScope(it, collectionScope) }
            is PqlCondition.Not -> getCrossScope(condition.condition, collectionScope)
        }
    }

    // Resolves cross-scope conditions by querying the outer scope and returning matching logIds
    private fun resolveCrossScopeConditions(
        conditions: List<PqlCondition>,
        collectionScope: PqlScope
    ): List<String> {
        // Determine the outer scope from conditions
        val outerScope = conditions.firstNotNullOfOrNull { getCrossScope(it, collectionScope) }
            ?: return emptyList()

        // Build a query for the outer scope
        val outerQuery = PqlQuery(
            collection = outerScope,
            projections = emptyList(),
            conditions = conditions,
            selectAll = true
        )
        val outerCouchQuery = translator.translate(outerQuery)
        val outerDocs = couchDB.findDocs(databaseName, outerCouchQuery)

        // Extract logIds from outer docs
        return outerDocs.mapNotNull { doc ->
            if (doc.isJsonObject) {
                val obj = doc.asJsonObject
                // For log docs, the _id IS the logId
                if (outerScope == PqlScope.LOG) {
                    obj.get("_id")?.asString
                } else {
                    obj.get("logId")?.asString
                }
            } else null
        }.distinct()
    }

    private fun processSelectQuery(queryText: String) {
        val output = executeQuery(queryText)
        println(gson.toJson(output))
    }
    
    private fun processDeleteQuery(queryText: String) {
        val deleteQuery = parser.parseDelete(queryText)
        val couchQuery = translator.translateDelete(deleteQuery)
        val deletedCount = couchDB.deleteDocs(databaseName, couchQuery)
        println("✅ Deleted $deletedCount document(s)")
    }

    private fun formatResponse(docs: JsonArray, query: PqlQuery): JsonElement {
        if (query.selectAll || query.projections.isEmpty()) {
            return docs
        }

        // Check if we have GROUP BY
        if (query.groupBy.isNotEmpty()) {
            return formatGroupedResponse(docs, query)
        }
        
        // Check if we have aggregation functions or scalar function 0 (now())
        // Also check if arithmetic expressions contain aggregation functions
        val hasAggregation = query.projections.any { 
            it.function is PqlFunction.AggregationFunction || 
            (it.arithmeticExpression != null && containsAggregationFunction(it.arithmeticExpression!!))
        }
        val hasScalar0Only = query.projections.all { 
            it.function is PqlFunction.ScalarFunction0 
        } && query.projections.isNotEmpty()
        
        if (hasAggregation || hasScalar0Only) {
            // Apply aggregation functions to all documents, or scalar0 functions once
            val result = JsonObject()
            query.projections.filterNot(PqlProjection::allAttributes).forEach { projection ->
                when {
                    projection.function is PqlFunction.AggregationFunction -> {
                        val function = projection.function as PqlFunction.AggregationFunction
                        val scope = projection.scope ?: query.collection
                        val attribute = projection.attribute
                        
                        val aggregatedValue = if (function.type == PqlFunction.Aggregation.COUNT && attribute == null) {
                            // COUNT(*) - count all documents
                            com.google.gson.JsonPrimitive(docs.size())
                        } else if (function.type == PqlFunction.Aggregation.COUNT && attribute != null) {
                            // COUNT(attribute) - count all documents (attribute value doesn't matter for COUNT)
                            com.google.gson.JsonPrimitive(docs.size())
                        } else if (attribute != null) {
                            // Aggregate over specific attribute
                            val fieldPath = translator.resolveField(scope, attribute)
                            val values = docs.mapNotNull { doc ->
                                if (doc.isJsonObject) {
                                    extractValue(doc.asJsonObject, fieldPath.segments)
                                } else null
                            }
                            applyAggregationFunction(function.type, values)
                        } else {
                            com.google.gson.JsonNull.INSTANCE
                        }
                        
                        result.add(projection.alias ?: projection.raw, aggregatedValue)
                    }
                    projection.arithmeticExpression != null && containsAggregationFunction(projection.arithmeticExpression!!) -> {
                        // Arithmetic expression with aggregation function - evaluate as aggregation
                        val value = evaluateArithmeticExpressionWithAggregation(projection.arithmeticExpression!!, docs, query.collection)
                        result.add(projection.alias ?: projection.raw, value ?: com.google.gson.JsonNull.INSTANCE)
                    }
                    projection.function is PqlFunction.ScalarFunction0 -> {
                        // Scalar function 0 (like now()) - apply once
                        val value = applyFunction(projection.function!!, JsonObject(), query.collection)
                        result.add(projection.alias ?: projection.raw, value ?: com.google.gson.JsonNull.INSTANCE)
                    }
                    else -> {
                        // Other functions or attributes - skip in aggregation mode
                    }
                }
            }
            return JsonArray().apply { add(result) }
        }

        // Regular projections with scalar functions or no functions
        val projected = JsonArray()
        for (element in docs) {
            if (!element.isJsonObject) continue
            val source = element.asJsonObject
            val projectionObject = JsonObject()
            query.projections.filterNot(PqlProjection::allAttributes).forEach { projection ->
                val value = when {
                    projection.arithmeticExpression != null -> {
                        evaluateArithmeticExpression(projection.arithmeticExpression!!, source, query.collection)
                    }
                    projection.function != null -> {
                        applyFunction(projection.function!!, source, query.collection)
                    }
                    else -> {
                        val scope = projection.scope ?: query.collection
                        val attribute = projection.attribute ?: return@forEach
                        val fieldPath = translator.resolveField(scope, attribute)
                        extractValue(source, fieldPath.segments)
                    }
                }
                projectionObject.add(projection.alias ?: projection.raw, value ?: JsonNull.INSTANCE)
            }
            projected.add(projectionObject)
        }
        return projected
    }
    
    private fun applyFunction(
        function: PqlFunction,
        source: JsonObject,
        collectionScope: pql.PqlScope
    ): JsonElement? {
        return when (function) {
            is PqlFunction.ScalarFunction0 -> {
                when (function.type) {
                    PqlFunction.Scalar0.NOW -> {
                        // Return current timestamp
                        com.google.gson.JsonPrimitive(java.time.Instant.now().toString())
                    }
                }
            }
            is PqlFunction.ScalarFunction1 -> {
                val scope = function.scope ?: collectionScope
                val attribute = function.attribute ?: return null
                val fieldPath = translator.resolveField(scope, attribute)
                val value = extractValue(source, fieldPath.segments) ?: return null
                
                applyScalarFunction(function.type, value)
            }
            is PqlFunction.AggregationFunction -> {
                // Aggregation functions are handled separately in formatResponse
                null
            }
        }
    }
    
    private fun applyScalarFunction(type: PqlFunction.Scalar1, value: JsonElement): JsonElement {
        return when (type) {
            PqlFunction.Scalar1.UPPER -> {
                if (value.isJsonPrimitive && value.asJsonPrimitive.isString) {
                    com.google.gson.JsonPrimitive(value.asString.uppercase())
                } else value
            }
            PqlFunction.Scalar1.LOWER -> {
                if (value.isJsonPrimitive && value.asJsonPrimitive.isString) {
                    com.google.gson.JsonPrimitive(value.asString.lowercase())
                } else value
            }
            PqlFunction.Scalar1.ROUND -> {
                if (value.isJsonPrimitive && value.asJsonPrimitive.isNumber) {
                    com.google.gson.JsonPrimitive(value.asDouble.toLong())
                } else value
            }
            PqlFunction.Scalar1.DATE -> {
                extractDatePart(value) { it.toLocalDate().toString() }
            }
            PqlFunction.Scalar1.TIME -> {
                extractDatePart(value) { 
                    it.toLocalTime().toString() 
                }
            }
            PqlFunction.Scalar1.YEAR -> {
                extractDatePart(value) { it.year.toString() }
            }
            PqlFunction.Scalar1.MONTH -> {
                extractDatePart(value) { it.monthValue.toString() }
            }
            PqlFunction.Scalar1.DAY -> {
                extractDatePart(value) { it.dayOfMonth.toString() }
            }
            PqlFunction.Scalar1.HOUR -> {
                extractDatePart(value) { it.hour.toString() }
            }
            PqlFunction.Scalar1.MINUTE -> {
                extractDatePart(value) { it.minute.toString() }
            }
            PqlFunction.Scalar1.SECOND -> {
                extractDatePart(value) { it.second.toString() }
            }
            PqlFunction.Scalar1.MILLISECOND -> {
                extractDatePart(value) { 
                    (it.nano / 1_000_000).toString() 
                }
            }
            PqlFunction.Scalar1.QUARTER -> {
                extractDatePart(value) { 
                    ((it.monthValue - 1) / 3 + 1).toString() 
                }
            }
            PqlFunction.Scalar1.DAYOFWEEK -> {
                extractDatePart(value) { 
                    it.dayOfWeek.value.toString() 
                }
            }
        }
    }
    
    private fun extractDatePart(value: JsonElement, extractor: (java.time.ZonedDateTime) -> String): JsonElement {
        if (!value.isJsonPrimitive || !value.asJsonPrimitive.isString) {
            return value
        }
        
        try {
            val dateStr = value.asString
            // Try ISO-8601 format with timezone
            val zonedDateTime = java.time.ZonedDateTime.parse(dateStr)
            return com.google.gson.JsonPrimitive(extractor(zonedDateTime))
        } catch (e: Exception) {
            // If parsing fails, return original value
            return value
        }
    }
    
    private fun applyAggregationFunction(
        type: PqlFunction.Aggregation,
        values: List<JsonElement>
    ): JsonElement {
        if (values.isEmpty()) {
            return when (type) {
                PqlFunction.Aggregation.COUNT -> com.google.gson.JsonPrimitive(0)
                else -> com.google.gson.JsonNull.INSTANCE
            }
        }
        
        // Try to parse as numbers first
        val numbers = values.mapNotNull { elem ->
            when {
                elem.isJsonPrimitive && elem.asJsonPrimitive.isNumber -> 
                    elem.asDouble
                elem.isJsonPrimitive && elem.asJsonPrimitive.isString -> 
                    elem.asString.toDoubleOrNull()
                else -> null
            }
        }
        
        // If we have numbers, use numeric aggregation
        if (numbers.isNotEmpty()) {
            return when (type) {
                PqlFunction.Aggregation.COUNT -> {
                    com.google.gson.JsonPrimitive(values.size)
                }
                PqlFunction.Aggregation.SUM -> {
                    com.google.gson.JsonPrimitive(numbers.sum())
                }
                PqlFunction.Aggregation.AVG -> {
                    com.google.gson.JsonPrimitive(numbers.average())
                }
                PqlFunction.Aggregation.MIN -> {
                    com.google.gson.JsonPrimitive(numbers.min())
                }
                PqlFunction.Aggregation.MAX -> {
                    com.google.gson.JsonPrimitive(numbers.max())
                }
            }
        }
        
        // Try to parse as dates/timestamps for MIN/MAX
        if (type == PqlFunction.Aggregation.MIN || type == PqlFunction.Aggregation.MAX) {
            val dates = values.mapNotNull { elem ->
                if (elem.isJsonPrimitive && elem.asJsonPrimitive.isString) {
                    try {
                        java.time.ZonedDateTime.parse(elem.asString)
                    } catch (e: Exception) {
                        null
                    }
                } else null
            }
            
            if (dates.isNotEmpty()) {
                val result = when (type) {
                    PqlFunction.Aggregation.MIN -> dates.min()
                    PqlFunction.Aggregation.MAX -> dates.max()
                    else -> return com.google.gson.JsonNull.INSTANCE
                }
                return com.google.gson.JsonPrimitive(result.toString())
            }
        }
        
        // Fallback: compare as strings for MIN/MAX
        if (type == PqlFunction.Aggregation.MIN || type == PqlFunction.Aggregation.MAX) {
            val strings = values.mapNotNull { elem ->
                if (elem.isJsonPrimitive) {
                    elem.asString
                } else null
            }
            
            if (strings.isNotEmpty()) {
                val result = when (type) {
                    PqlFunction.Aggregation.MIN -> strings.min()
                    PqlFunction.Aggregation.MAX -> strings.max()
                    else -> return com.google.gson.JsonNull.INSTANCE
                }
                return com.google.gson.JsonPrimitive(result)
            }
        }
        
        // If nothing worked, return null
        return com.google.gson.JsonNull.INSTANCE
    }

    private fun extractValue(source: JsonObject, segments: List<String>): JsonElement? {
        var current: JsonElement = source
        for (segment in segments) {
            if (!current.isJsonObject) return null
            current = current.asJsonObject.get(segment) ?: return null
        }
        return current
    }
    
    private fun formatGroupedResponse(docs: JsonArray, query: PqlQuery): JsonElement {
        // Group documents by GROUP BY fields
        val groups = mutableMapOf<String, MutableList<JsonObject>>()
        
        for (element in docs) {
            if (!element.isJsonObject) continue
            val doc = element.asJsonObject
            
            // Build group key from GROUP BY fields
            val groupKey = query.groupBy.joinToString("|") { groupField ->
                val fieldPath = translator.resolveField(groupField.scope, groupField.attribute)
                val value = extractValue(doc, fieldPath.segments)
                value?.asString ?: "null"
            }
            
            groups.getOrPut(groupKey) { mutableListOf() }.add(doc)
        }
        
        // Process each group
        val result = JsonArray()
        groups.forEach { (_, groupDocs) ->
            val groupResult = JsonObject()
            
            // Track which GROUP BY fields we've already added
            val addedGroupByFields = mutableSetOf<String>()
            
            // Process projections - first add GROUP BY fields, then aggregations
            query.projections.filterNot(PqlProjection::allAttributes).forEach { projection ->
                // Check if this projection is a GROUP BY field (not a function, not aggregation)
                val isGroupByField = projection.function == null && projection.arithmeticExpression == null && 
                    projection.attribute != null &&
                    query.groupBy.any { 
                        it.scope == (projection.scope ?: query.collection) && 
                        it.attribute == projection.attribute 
                    }
                
                if (isGroupByField && !addedGroupByFields.contains(projection.attribute!!)) {
                    // Add GROUP BY field only once
                    val fieldPath = translator.resolveField(projection.scope ?: query.collection, projection.attribute!!)
                    val value = extractValue(groupDocs[0], fieldPath.segments)
                    addedGroupByFields.add(projection.attribute!!)
                    groupResult.add(projection.alias ?: projection.attribute!!, value ?: JsonNull.INSTANCE)
                }
            }
            
            // Now process aggregations and other non-GROUP BY projections
            query.projections.filterNot(PqlProjection::allAttributes).forEach { projection ->
                // Skip GROUP BY fields (already added)
                val isGroupByField = projection.function == null && projection.arithmeticExpression == null && 
                    projection.attribute != null &&
                    query.groupBy.any { 
                        it.scope == (projection.scope ?: query.collection) && 
                        it.attribute == projection.attribute 
                    }
                
                if (!isGroupByField) {
                    val value = when {
                        projection.function is PqlFunction.AggregationFunction -> {
                            val func = projection.function as PqlFunction.AggregationFunction
                            val scope = projection.scope ?: query.collection
                            val attribute = projection.attribute
                            
                            when {
                                func.type == PqlFunction.Aggregation.COUNT && attribute == null -> {
                                    // COUNT(*) - count all documents in group
                                    com.google.gson.JsonPrimitive(groupDocs.size)
                                }
                                func.type == PqlFunction.Aggregation.COUNT && attribute != null -> {
                                    // COUNT(attribute) - count all documents in group
                                    com.google.gson.JsonPrimitive(groupDocs.size)
                                }
                                attribute != null -> {
                                    val fieldPath = translator.resolveField(scope, attribute)
                                    val values = groupDocs.mapNotNull { doc ->
                                        extractValue(doc, fieldPath.segments)
                                    }
                                    applyAggregationFunction(func.type, values)
                                }
                                else -> {
                                    com.google.gson.JsonNull.INSTANCE
                                }
                            }
                        }
                        projection.arithmeticExpression != null -> {
                            // For arithmetic expressions in GROUP BY, evaluate on first document
                            evaluateArithmeticExpression(projection.arithmeticExpression!!, groupDocs[0], query.collection)
                        }
                        projection.function != null -> {
                            // Scalar functions - apply to first document
                            applyFunction(projection.function!!, groupDocs[0], query.collection)
                        }
                        else -> {
                            // This shouldn't happen for non-GROUP BY fields, but handle it
                            val scope = projection.scope ?: query.collection
                            val attribute = projection.attribute
                            if (attribute != null) {
                                val fieldPath = translator.resolveField(scope, attribute)
                                extractValue(groupDocs[0], fieldPath.segments)
                            } else {
                                null
                            }
                        }
                    }
                    
                    // Always add aggregation functions and other non-GROUP BY fields
                    groupResult.add(projection.alias ?: projection.raw, value ?: JsonNull.INSTANCE)
                }
            }
            
            result.add(groupResult)
        }
        
        // Apply LIMIT and OFFSET
        val limitedResult = JsonArray()
        val offsetVal = query.offsets.firstOrNull { it.scope == query.collection || it.scope == null }?.value ?: 0
        val limitVal = query.limits.firstOrNull { it.scope == query.collection || it.scope == null }?.value
        val startIndex = offsetVal
        val endIndex = if (limitVal != null) startIndex + limitVal else result.size()
        
        for (i in startIndex until minOf(endIndex, result.size())) {
            limitedResult.add(result[i])
        }
        
        return limitedResult
    }
    
    private fun evaluateArithmeticExpression(
        expr: PqlArithmeticExpression,
        source: JsonObject,
        collectionScope: pql.PqlScope
    ): JsonElement? {
        return when (expr) {
            is PqlArithmeticExpression.Attribute -> {
                val fieldPath = translator.resolveField(expr.scope, expr.attribute)
                extractValue(source, fieldPath.segments)
            }
            is PqlArithmeticExpression.Literal -> {
                when (val value = expr.value) {
                    is Number -> com.google.gson.JsonPrimitive(value)
                    is String -> com.google.gson.JsonPrimitive(value)
                    is Boolean -> com.google.gson.JsonPrimitive(value)
                    else -> com.google.gson.JsonPrimitive(value.toString())
                }
            }
            is PqlArithmeticExpression.Binary -> {
                val left = evaluateArithmeticExpression(expr.left, source, collectionScope)
                val right = evaluateArithmeticExpression(expr.right, source, collectionScope)
                
                if (left == null || right == null) {
                    return com.google.gson.JsonNull.INSTANCE
                }
                
                // Convert to numbers
                val leftNum = toNumber(left)
                val rightNum = toNumber(right)
                
                if (leftNum == null || rightNum == null) {
                    return com.google.gson.JsonNull.INSTANCE
                }
                
                val result = when (expr.operator) {
                    ArithmeticOperator.ADD -> leftNum + rightNum
                    ArithmeticOperator.SUB -> leftNum - rightNum
                    ArithmeticOperator.MUL -> leftNum * rightNum
                    ArithmeticOperator.DIV -> {
                        if (rightNum == 0.0) {
                            return com.google.gson.JsonNull.INSTANCE
                        }
                        leftNum / rightNum
                    }
                }
                
                com.google.gson.JsonPrimitive(result)
            }
            is PqlArithmeticExpression.FunctionCall -> {
                applyFunction(expr.function, source, collectionScope)
            }
        }
    }
    
    private fun toNumber(elem: JsonElement): Double? {
        return when {
            elem.isJsonPrimitive && elem.asJsonPrimitive.isNumber -> 
                elem.asDouble
            elem.isJsonPrimitive && elem.asJsonPrimitive.isString -> 
                elem.asString.toDoubleOrNull()
            else -> null
        }
    }
    
    private fun containsAggregationFunction(expr: PqlArithmeticExpression): Boolean {
        return when (expr) {
            is PqlArithmeticExpression.FunctionCall -> {
                expr.function is PqlFunction.AggregationFunction
            }
            is PqlArithmeticExpression.Binary -> {
                containsAggregationFunction(expr.left) || containsAggregationFunction(expr.right)
            }
            is PqlArithmeticExpression.Attribute -> false
            is PqlArithmeticExpression.Literal -> false
        }
    }
    
    private fun evaluateArithmeticExpressionWithAggregation(
        expr: PqlArithmeticExpression,
        docs: JsonArray,
        collectionScope: pql.PqlScope
    ): JsonElement? {
        return when (expr) {
            is PqlArithmeticExpression.FunctionCall -> {
                if (expr.function is PqlFunction.AggregationFunction) {
                    val func = expr.function as PqlFunction.AggregationFunction
                    val scope = func.scope ?: collectionScope
                    val attribute = func.attribute
                    
                    when {
                        func.type == PqlFunction.Aggregation.COUNT && attribute == null -> {
                            com.google.gson.JsonPrimitive(docs.size())
                        }
                        func.type == PqlFunction.Aggregation.COUNT && attribute != null -> {
                            com.google.gson.JsonPrimitive(docs.size())
                        }
                        attribute != null -> {
                            val fieldPath = translator.resolveField(scope, attribute)
                            val values = docs.mapNotNull { doc ->
                                if (doc.isJsonObject) {
                                    extractValue(doc.asJsonObject, fieldPath.segments)
                                } else null
                            }
                            applyAggregationFunction(func.type, values)
                        }
                        else -> com.google.gson.JsonNull.INSTANCE
                    }
                } else {
                    // Scalar function - apply to first document (or empty object)
                    applyFunction(expr.function, JsonObject(), collectionScope)
                }
            }
            is PqlArithmeticExpression.Binary -> {
                val left = evaluateArithmeticExpressionWithAggregation(expr.left, docs, collectionScope)
                val right = evaluateArithmeticExpressionWithAggregation(expr.right, docs, collectionScope)
                
                if (left == null || right == null) {
                    return com.google.gson.JsonNull.INSTANCE
                }
                
                val leftNum = toNumber(left)
                val rightNum = toNumber(right)
                
                if (leftNum == null || rightNum == null) {
                    return com.google.gson.JsonNull.INSTANCE
                }
                
                val result = when (expr.operator) {
                    ArithmeticOperator.ADD -> leftNum + rightNum
                    ArithmeticOperator.SUB -> leftNum - rightNum
                    ArithmeticOperator.MUL -> leftNum * rightNum
                    ArithmeticOperator.DIV -> {
                        if (rightNum == 0.0) {
                            return com.google.gson.JsonNull.INSTANCE
                        }
                        leftNum / rightNum
                    }
                }
                
                com.google.gson.JsonPrimitive(result)
            }
            is PqlArithmeticExpression.Attribute -> {
                // This shouldn't happen in aggregation context, but handle it
                val fieldPath = translator.resolveField(expr.scope, expr.attribute)
                // Return first document's value or null
                if (docs.size() > 0 && docs[0].isJsonObject) {
                    extractValue(docs[0].asJsonObject, fieldPath.segments)
                } else null
            }
            is PqlArithmeticExpression.Literal -> {
                when (val value = expr.value) {
                    is Number -> com.google.gson.JsonPrimitive(value)
                    is String -> com.google.gson.JsonPrimitive(value)
                    is Boolean -> com.google.gson.JsonPrimitive(value)
                    else -> com.google.gson.JsonPrimitive(value.toString())
                }
            }
        }
    }
}


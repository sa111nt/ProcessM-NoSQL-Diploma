package pql.translator

import pql.model.PqlScope

data class FieldPath(val segments: List<String>) {
    fun toDotPath(): String = segments.joinToString(".")
}

class PqlFieldMapper {
    private val standardAliases = mapOf(
        "id" to "identity:id",
        "name" to "concept:name",
        "instance" to "concept:instance",
        "timestamp" to "time:timestamp",
        "resource" to "org:resource",
        "role" to "org:role",
        "group" to "org:group",
        "transition" to "lifecycle:transition",
        "state" to "lifecycle:state",
        "model" to "lifecycle:model",
        "total" to "cost:total",
        "currency" to "cost:currency",
        "amount" to "cost:amount",
        "driver" to "cost:driver",
        "type" to "cost:type",
        "drivers" to "cost:drivers",
        "modelreference" to "semantic:modelReference",
        "moves" to "artifactlifecycle:moves",
        "level" to "micro:level",
        "parentid" to "micro:parentId",
        "length" to "micro:length",
        "localhost" to "swcomm:localHost",
        "localport" to "swcomm:localPort",
        "remotehost" to "swcomm:remoteHost",
        "remoteport" to "swcomm:remotePort",
        "hasdata" to "swevent:hasData",
        "hasexception" to "swevent:hasException",
        "returnvalue" to "swevent:returnValue",
        "appname" to "swevent:appName",
        "threadid" to "swevent:threadId",
        "nanotime" to "swevent:nanotime",
        "exthrown" to "swevent:exThrown",
        "excaught" to "swevent:exCaught",
        "cputotaluser" to "swtelemetry:cpuTotalUser",
        "cpuloaduser" to "swtelemetry:cpuLoadUser",
        "memoryused" to "swtelemetry:memoryUsed",
        "memorytotal" to "swtelemetry:memoryTotal",
        "traces_total" to "meta_general:traces_total",
        "events_total" to "meta_general:events_total",
        "events_average" to "meta_general:events_average",
        "log_duration" to "meta_time:log_duration",
        "duration_average" to "meta_time:duration_average",
        "version" to "xes:version",
        "features" to "xes:features"
    )

    fun getStandardName(attribute: String): String {
        if (attribute.startsWith("[")) {
            return attribute
        }

        val cleanAttr = attribute.removePrefix("^^").removePrefix("^")
        val lowerAttr = cleanAttr.lowercase()

        if (lowerAttr.startsWith("c:") || lowerAttr.startsWith("classifier:")) {
            return attribute
        }

        val resolved = standardAliases[lowerAttr] ?: cleanAttr
        val hoistingPrefix = attribute.takeWhile { it == '^' }
        return hoistingPrefix + resolved
    }

    fun resolve(scope: PqlScope, attribute: String): FieldPath {
        val isLiteral = attribute.startsWith("[")

        if (isLiteral) {
            val clean = attribute.removePrefix("[").removeSuffix("]")
                .removePrefix("e:").removePrefix("t:").removePrefix("l:")
            return when (scope) {
                PqlScope.LOG -> FieldPath(listOf("log_attributes", clean))
                PqlScope.TRACE -> FieldPath(listOf("xes_attributes", clean))
                PqlScope.EVENT -> FieldPath(listOf("xes_attributes", clean))
            }
        }

        val standardName = getStandardName(attribute).removePrefix("^^").removePrefix("^")
        val lowerName = standardName.lowercase()

        if (lowerName == "identity:id") {
            return when (scope) {
                PqlScope.LOG -> FieldPath(listOf("log_attributes", "identity:id"))
                PqlScope.TRACE -> FieldPath(listOf("xes_attributes", "identity:id"))
                PqlScope.EVENT -> FieldPath(listOf("xes_attributes", "identity:id"))
            }
        }

        if (lowerName == "_id" || lowerName == "logid" || lowerName == "traceid" || lowerName == "source" || lowerName == "importtimestamp") {
            return FieldPath(listOf(standardName))
        }

        if (lowerName == "concept:name") {
            return when (scope) {
                PqlScope.EVENT -> FieldPath(listOf("activity"))
                PqlScope.TRACE -> FieldPath(listOf("xes_attributes", "concept:name"))
                PqlScope.LOG -> FieldPath(listOf("log_attributes", "concept:name"))
            }
        }

        if (lowerName == "time:timestamp") {
            return when (scope) {
                PqlScope.EVENT -> FieldPath(listOf("timestamp"))
                PqlScope.TRACE -> FieldPath(listOf("xes_attributes", "time:timestamp"))
                PqlScope.LOG -> FieldPath(listOf("log_attributes", "time:timestamp"))
            }
        }

        return when (scope) {
            PqlScope.LOG -> FieldPath(listOf("log_attributes", standardName))
            PqlScope.TRACE -> FieldPath(listOf("xes_attributes", standardName))
            PqlScope.EVENT -> FieldPath(listOf("xes_attributes", standardName))
        }
    }

    fun resolveForCondition(conditionScope: PqlScope, collectionScope: PqlScope, attribute: String): FieldPath {
        val isLiteral = attribute.startsWith("[")
        if (isLiteral) {
            return resolve(conditionScope, attribute)
        }

        val cleanAttr = attribute.removePrefix("[").removeSuffix("]")
        val strippedAttr = cleanAttr.removePrefix("e:").removePrefix("t:").removePrefix("l:")

        val standardName = getStandardName(strippedAttr).removePrefix("^^").removePrefix("^")
        val lowerName = standardName.lowercase()

        if (lowerName == "_id") {
            return if (conditionScope == collectionScope) FieldPath(listOf("_id"))
            else if (conditionScope == PqlScope.LOG) FieldPath(listOf("logId"))
            else if (conditionScope == PqlScope.TRACE) FieldPath(listOf("traceId"))
            else FieldPath(listOf("_id"))
        }

        if (conditionScope != collectionScope) {
            val scopePrefix = when (conditionScope) { PqlScope.EVENT -> "e:"; PqlScope.TRACE -> "t:"; PqlScope.LOG -> "l:" }
            return FieldPath(listOf(scopePrefix + standardName))
        }

        return resolve(conditionScope, strippedAttr)
    }

    fun getImplicitClassifierMapping(classifierBase: String): List<String> {
        return when (classifierBase.lowercase()) {
            "event name" -> listOf("concept:name", "lifecycle:transition")
            "resource" -> listOf("org:resource")
            else -> listOf(getStandardName(classifierBase))
        }
    }
}
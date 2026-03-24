package pql.translator

import pql.model.PqlScope

data class FieldPath(val segments: List<String>) {
    fun toDotPath(): String = segments.joinToString(".")
}

class PqlFieldMapper {
    private val standardAliases = mapOf(
        // Identity & Concept & Time
        "id" to "identity:id",
        "name" to "concept:name",
        "instance" to "concept:instance",
        "timestamp" to "time:timestamp",

        // Organizational & Lifecycle
        "resource" to "org:resource",
        "role" to "org:role",
        "group" to "org:group",
        "transition" to "lifecycle:transition",
        "state" to "lifecycle:state",
        "model" to "lifecycle:model",

        // Cost (Pełna specyfikacja z repozytorium)
        "total" to "cost:total",
        "currency" to "cost:currency",
        "amount" to "cost:amount",
        "driver" to "cost:driver",
        "type" to "cost:type",
        "drivers" to "cost:drivers",

        // Semantic & ArtifactLifecycle
        "modelreference" to "semantic:modelReference",
        "moves" to "artifactlifecycle:moves",

        // Micro
        "level" to "micro:level",
        "parentid" to "micro:parentId",
        "length" to "micro:length",

        // Software Communication
        "localhost" to "swcomm:localHost",
        "localport" to "swcomm:localPort",
        "remotehost" to "swcomm:remoteHost",
        "remoteport" to "swcomm:remotePort",

        // Software Event (Kluczowe)
        "hasdata" to "swevent:hasData",
        "hasexception" to "swevent:hasException",
        "returnvalue" to "swevent:returnValue",
        "appname" to "swevent:appName",
        "threadid" to "swevent:threadId",
        "nanotime" to "swevent:nanotime",
        "exthrown" to "swevent:exThrown",
        "excaught" to "swevent:exCaught",

        // Software Telemetry
        "cputotaluser" to "swtelemetry:cpuTotalUser",
        "cpuloaduser" to "swtelemetry:cpuLoadUser",
        "memoryused" to "swtelemetry:memoryUsed",
        "memorytotal" to "swtelemetry:memoryTotal",

        // MetaData (Najczęściej używane z meta_general i meta_time)
        "traces_total" to "meta_general:traces_total",
        "events_total" to "meta_general:events_total",
        "events_average" to "meta_general:events_average",
        "log_duration" to "meta_time:log_duration",
        "duration_average" to "meta_time:duration_average",

        // Systemowe XES
        "version" to "xes:version",
        "features" to "xes:features"
    )

    fun getStandardName(attribute: String): String {
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
package input.parsers

import model.Event
import model.Trace
import java.io.File
import java.util.UUID
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants
import javax.xml.stream.XMLStreamReader

interface XESInputStream : Iterator<XESComponent>

class StaxXesParser(private val reader: XMLStreamReader) : XESInputStream {

    constructor(file: File) : this(
        XMLInputFactory.newDefaultFactory().createXMLStreamReader(file.inputStream())
    )

    private var nextComponent: XESComponent? = null
    private val primitiveTypes = setOf("string", "date", "int", "float", "boolean", "id")

    override fun hasNext(): Boolean {
        if (nextComponent != null) return true

        if (reader.eventType == XMLStreamConstants.START_ELEMENT && reader.localName == "trace") {
            nextComponent = XESTrace(parseTrace())
            return true
        }

        while (reader.hasNext()) {
            when (reader.next()) {
                XMLStreamConstants.START_ELEMENT -> {
                    val name = reader.localName
                    if (name == "log") {
                        nextComponent = parseLogHeader()
                        return true
                    } else if (name == "trace") {
                        nextComponent = XESTrace(parseTrace())
                        return true
                    }
                }
                XMLStreamConstants.END_DOCUMENT -> return false
            }
        }
        return false
    }

    override fun next(): XESComponent {
        if (nextComponent != null || hasNext()) {
            val result = nextComponent!!
            nextComponent = null
            return result
        }
        throw NoSuchElementException()
    }

    private fun parseAttribute(prefix: String = ""): List<Pair<String, Any?>> {
        val tag = reader.localName
        val rawKey = reader.getAttributeValue(null, "key") ?: "unknown"
        val key = if (prefix.isEmpty()) rawKey else "$prefix:$rawKey"
        val results = mutableListOf<Pair<String, Any?>>()

        if (tag in primitiveTypes) {
            val value = castValue(tag, reader.getAttributeValue(null, "value"))
            results.add(key to value)
        }

        while (reader.hasNext()) {
            reader.next()
            if (reader.isStartElement) {
                val childTag = reader.localName
                if (childTag in primitiveTypes || childTag == "list" || childTag == "container") {
                    results.addAll(parseAttribute(key))
                }
            } else if (reader.isEndElement && reader.localName == tag) {
                break
            }
        }
        return results
    }

    private fun parseLogHeader(): XESLogAttributes {
        val attributes = mutableMapOf<String, Any?>()
        val extensions = mutableListOf<XesExtension>()
        val globals = mutableListOf<XesGlobal>()
        val classifiersDef = mutableListOf<XesClassifier>()
        val classifiersMap = mutableMapOf<String, List<String>>()

        while (reader.hasNext()) {
            reader.next()
            if (reader.isStartElement) {
                val name = reader.localName

                if (name == "extension") {
                    val extName = reader.getAttributeValue(null, "name") ?: ""
                    val extPrefix = reader.getAttributeValue(null, "prefix") ?: ""
                    val extUri = reader.getAttributeValue(null, "uri") ?: ""
                    extensions.add(XesExtension(extName, extPrefix, extUri))
                } else if (name == "global") {
                    val scope = reader.getAttributeValue(null, "scope") ?: ""
                    val globalAttrs = mutableMapOf<String, Any?>()
                    var readingGlobal = true
                    while (reader.hasNext() && readingGlobal) {
                        reader.next()
                        if (reader.isStartElement) {
                            val tag = reader.localName
                            if (tag in primitiveTypes || tag == "list" || tag == "container") {
                                parseAttribute().forEach { globalAttrs[it.first] = it.second }
                            }
                        } else if (reader.isEndElement && reader.localName == "global") {
                            readingGlobal = false
                        }
                    }
                    globals.add(XesGlobal(scope, globalAttrs))
                } else if (name == "classifier") {
                    val cName = reader.getAttributeValue(null, "name") ?: ""
                    val cKeys = reader.getAttributeValue(null, "keys") ?: ""
                    val keysList = cKeys.split(" ", ",").filter { it.isNotBlank() }.map { it.trim('\'', '"') }
                    classifiersDef.add(XesClassifier(cName, keysList))
                    classifiersMap[cName] = keysList
                } else if (name in primitiveTypes || name == "list" || name == "container") {
                    parseAttribute().forEach { attributes[it.first] = it.second }
                } else if (name == "trace") {
                    return XESLogAttributes(attributes, extensions, globals, classifiersDef, classifiersMap)
                }
            } else if (reader.isEndElement && reader.localName == "log") {
                return XESLogAttributes(attributes, extensions, globals, classifiersDef, classifiersMap)
            }
        }
        return XESLogAttributes(attributes, extensions, globals, classifiersDef, classifiersMap)
    }

    private fun parseTrace(): Trace {
        val traceAttributes = mutableMapOf<String, Any?>()
        val events = mutableListOf<Event>()
        val currentTraceId = UUID.randomUUID()

        while (reader.hasNext()) {
            reader.next()
            if (reader.isStartElement) {
                val name = reader.localName
                if (name in primitiveTypes || name == "list" || name == "container") {
                    parseAttribute().forEach { attr ->
                        traceAttributes[attr.first] = attr.second
                    }
                } else if (name == "event") {
                    events.add(parseEvent(currentTraceId))
                }
            } else if (reader.isEndElement && reader.localName == "trace") break
        }

        return Trace(
            id = currentTraceId,
            attributes = traceAttributes,
            events = events
        )
    }

    private fun parseEvent(traceId: UUID): Event {
        val attrs = mutableMapOf<String, Any?>()
        var name: String? = null
        var timestamp: String? = null

        var reading = true
        while (reader.hasNext() && reading) {
            reader.next()
            if (reader.isStartElement) {
                val tag = reader.localName
                if (tag in primitiveTypes || tag == "list" || tag == "container") {
                    parseAttribute().forEach { attr ->
                        when (attr.first) {
                            "concept:name" -> name = attr.second?.toString()
                            "time:timestamp" -> timestamp = attr.second?.toString()
                        }
                        attrs[attr.first] = attr.second
                    }
                }
            } else if (reader.isEndElement && reader.localName == "event") reading = false
        }

        return Event(
            id = UUID.randomUUID(),
            traceId = traceId,
            name = name,
            timestamp = timestamp,
            attributes = attrs
        )
    }

    private fun castValue(tag: String, value: String?): Any? {
        if (value == null) return null
        return try {
            when (tag) {
                "string", "date" -> value
                "id" -> UUID.fromString(value)
                "int" -> value.toLong()
                "float" -> value.toDouble()
                "boolean" -> value.toBooleanStrict()
                else -> value
            }
        } catch (e: Exception) {
            if (tag == "id") null else value
        }
    }
}
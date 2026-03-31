package pql.parser

import org.antlr.v4.runtime.CharStream
import org.antlr.v4.runtime.misc.Interval

class CaseChangingCharStream(
    private val stream: CharStream,
    private val upper: Boolean
) : CharStream {

    override fun getText(interval: Interval): String {
        return stream.getText(interval)
    }

    override fun consume() {
        stream.consume()
    }

    override fun LA(i: Int): Int {
        val c = stream.LA(i)

        // "All keywords, identifiers, and comparisons with values in PQL are case-sensitive."
        // We are supposed to preserve the original case to strictly comply with the PQL specification,
        // so this wrapper acts as a pure pass-through and does not modify the characters.
        return c
    }

    override fun mark(): Int {
        return stream.mark()
    }

    override fun release(marker: Int) {
        stream.release(marker)
    }

    override fun index(): Int {
        return stream.index()
    }

    override fun seek(index: Int) {
        stream.seek(index)
    }

    override fun size(): Int {
        return stream.size()
    }

    override fun getSourceName(): String {
        return stream.sourceName
    }
}
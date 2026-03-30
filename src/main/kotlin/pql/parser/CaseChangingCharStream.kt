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
        if (c <= 0) {
            return c
        }
        if (!upper) {
            return Character.toLowerCase(c)
        }
        return Character.toUpperCase(c)
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
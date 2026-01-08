package pql.parser

import org.antlr.v4.runtime.CharStream
import org.antlr.v4.runtime.misc.Interval

/**
 * Wrapper na CharStream, który zamienia wszystkie znaki na małe litery (Upper Case -> Lower Case)
 * w locie. Pozwala to używać gramatyki Case-Sensitive (np. wymagającej 'select')
 * z zapytaniami Case-Insensitive (np. 'SELECT').
 */
class CaseChangingCharStream(
    private val stream: CharStream,
    private val upper: Boolean
) : CharStream {

    override fun getText(interval: Interval): String {
        return stream.getText(interval) // Zwracamy oryginał (żeby stringi 'A' nie zamieniły się na 'a')
    }

    override fun consume() {
        stream.consume()
    }

    override fun LA(i: Int): Int {
        val c = stream.LA(i)
        if (c <= 0) {
            return c
        }
        // Jeśli upper=false, zamieniamy na małe litery
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
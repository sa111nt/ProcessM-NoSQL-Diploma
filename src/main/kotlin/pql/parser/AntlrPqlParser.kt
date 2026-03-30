package pql.parser

import org.antlr.v4.runtime.*
import org.antlr.v4.runtime.tree.ParseTreeWalker
import pql.model.PqlDeleteQuery
import ql.QLParser
import ql.QLLexer

class AntlrPqlParser {

    fun parse(queryText: String): Any {
        val charStream = CharStreams.fromString(queryText)
        val caseInsensitiveStream = CaseChangingCharStream(charStream, upper = false)

        val lexer = QLLexer(caseInsensitiveStream)
        lexer.removeErrorListeners()
        lexer.addErrorListener(ThrowingErrorListener)

        val tokens = CommonTokenStream(lexer)
        val parser = QLParser(tokens)
        parser.removeErrorListeners()
        parser.addErrorListener(ThrowingErrorListener)

        val tree = parser.query()

        val walker = ParseTreeWalker()
        val listener = PqlQueryListener()

        walker.walk(listener, tree)

        if (listener.isDelete()) {
            return listener.buildDeleteQuery()
        }
        return listener.buildQuery()
    }

    fun parseDelete(queryText: String): PqlDeleteQuery {
        val result = parse(queryText)
        if (result is PqlDeleteQuery) return result

        throw IllegalArgumentException("Expected DELETE query, but got ${result::class.simpleName}")
    }
}

object ThrowingErrorListener : BaseErrorListener() {
    override fun syntaxError(
        recognizer: Recognizer<*, *>?,
        offendingSymbol: Any?,
        line: Int,
        charPositionInLine: Int,
        msg: String,
        e: RecognitionException?
    ) {
        throw IllegalArgumentException("Parse error: Syntax error at line $line:$charPositionInLine - $msg")
    }
}
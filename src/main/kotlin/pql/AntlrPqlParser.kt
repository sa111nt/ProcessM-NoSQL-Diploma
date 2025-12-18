package pql

import org.antlr.v4.runtime.*
import org.antlr.v4.runtime.tree.ParseTreeWalker
import ql.QLParser
import ql.QLLexer

/**
 * ANTLR-based PQL parser that replaces the regex-based parser.
 * This parser uses the ANTLR grammar to parse PQL queries.
 */
class AntlrPqlParser {
    
    fun parse(queryText: String): PqlQuery {
        val stream: CharStream = CharStreams.fromString(queryText)
        val lexer = QLLexer(stream)
        val tokens = CommonTokenStream(lexer)
        val parser = QLParser(tokens)
        
        // Custom error listener to collect errors
        val errorListener = object : BaseErrorListener() {
            val errors = mutableListOf<String>()
            override fun syntaxError(
                recognizer: Recognizer<*, *>?,
                offendingSymbol: Any?,
                line: Int,
                charPositionInLine: Int,
                msg: String,
                e: RecognitionException?
            ) {
                errors.add("Syntax error at line $line:$charPositionInLine - $msg")
            }
        }
        
        lexer.removeErrorListeners()
        parser.removeErrorListeners()
        lexer.addErrorListener(errorListener)
        parser.addErrorListener(errorListener)
        
        val tree = parser.query()
        
        // Check for parsing errors
        if (errorListener.errors.isNotEmpty()) {
            throw IllegalArgumentException("Parse error: ${errorListener.errors.joinToString("; ")}")
        }
        
        val walker = ParseTreeWalker()
        val listener = PqlQueryListener()
        
        walker.walk(listener, tree)
        
        return listener.buildQuery()
    }
    
    fun parseDelete(queryText: String): PqlDeleteQuery {
        val stream: CharStream = CharStreams.fromString(queryText)
        val lexer = QLLexer(stream)
        val tokens = CommonTokenStream(lexer)
        val parser = QLParser(tokens)
        
        // Custom error listener to collect errors
        val errorListener = object : BaseErrorListener() {
            val errors = mutableListOf<String>()
            override fun syntaxError(
                recognizer: Recognizer<*, *>?,
                offendingSymbol: Any?,
                line: Int,
                charPositionInLine: Int,
                msg: String,
                e: RecognitionException?
            ) {
                errors.add("Syntax error at line $line:$charPositionInLine - $msg")
            }
        }
        
        lexer.removeErrorListeners()
        parser.removeErrorListeners()
        lexer.addErrorListener(errorListener)
        parser.addErrorListener(errorListener)
        
        val tree = parser.query()
        
        // Check for parsing errors
        if (errorListener.errors.isNotEmpty()) {
            throw IllegalArgumentException("Parse error: ${errorListener.errors.joinToString("; ")}")
        }
        
        val walker = ParseTreeWalker()
        val listener = PqlQueryListener()
        
        walker.walk(listener, tree)
        
        if (!listener.isDelete()) {
            error("Expected DELETE query")
        }
        
        return listener.buildDeleteQuery()
    }
}


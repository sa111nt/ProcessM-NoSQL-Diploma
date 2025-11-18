import app.LogImporter
import app.PqlInterpreter
import db.CouchDBManager

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        println("❌ Please provide a path to a log file/ZIP or use '--pql' to start the query interpreter.")
        return
    }

    if (args.size == 1 && args[0] == "--pql") {
        val couch = CouchDBManager()
        val interpreter = PqlInterpreter(couch)
        interpreter.start()
        return
    }

    LogImporter.import(args[0])
}
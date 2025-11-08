import app.LogImporter

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        println("❌ Please provide a path to a log file or ZIP as an argument.")
        return
    }

    LogImporter.import(args[0])
}
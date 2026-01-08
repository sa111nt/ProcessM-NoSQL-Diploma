package app

import db.CouchDBManager
import java.util.Scanner
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    println("=== ProcessM NoSQL Importer & Query Engine ===")

    val dbName = "event_logs" // Nazwa bazy wspólna dla importu i zapytań

    // 1. INICJALIZACJA POŁĄCZENIA (Robimy to RAZ na początku)
    val dbManager = CouchDBManager()

    try {
        // Upewniamy się, że baza istnieje
        try {
            dbManager.createDb(dbName)
        } catch (_: Exception) { /* Ignorujemy, jeśli baza już istnieje */ }

        // 2. IMPORT DANYCH (Jeśli podano plik w argumentach)
        if (args.isNotEmpty()) {
            val filePath = args[0]
            println("\n📂 Wykryto argument startowy: $filePath")
            println("🚀 Uruchamiam LogImporter z istniejącym połączeniem...")

            // Przekazujemy 'dbName' oraz 'dbManager' do Importera.
            LogImporter.import(filePath, dbName, dbManager)

            println("✅ Import zakończony. Przełączanie w tryb interaktywny...")
        } else {
            println("ℹ️ Brak pliku do importu. Tryb tylko do odczytu.")
        }

        // 3. TRYB INTERAKTYWNY (PQL Console)
        // Interpreter korzysta z tego samego dbManagera co import!
        val interpreter = PqlInterpreter(dbManager, dbName)
        val scanner = Scanner(System.`in`)

        println("\n--- Konsola PQL ($dbName) ---")
        println("Wpisz zapytanie (np. SELECT * LIMIT 5) lub 'exit' aby wyjść.")

        while (true) {
            print("\nPQL> ")
            // Zabezpieczenie przed Ctrl+D / zamknięciem strumienia
            if (!scanner.hasNextLine()) break

            val input = scanner.nextLine().trim()

            if (input.equals("exit", ignoreCase = true)) {
                break
            }
            if (input.isEmpty()) continue

            try {
                val startTime = System.currentTimeMillis()

                // Wykonanie zapytania
                val resultJson = interpreter.execute(input)

                val duration = System.currentTimeMillis() - startTime
                println("Wynik (${duration}ms):\n$resultJson")

            } catch (e: Exception) {
                println("❌ Błąd: ${e.message}")
            }
        }

    } catch (e: Exception) {
        println("🔥 Błąd krytyczny aplikacji: ${e.message}")
        e.printStackTrace()
    } finally {
        // 4. SPRZĄTANIE
        // To wykona się zawsze, nawet jak program się wywali podczas importu.
        println("Zamykanie połączenia z CouchDB...")
        dbManager.close()
    }

    println("Do widzenia!")
    exitProcess(0)
}
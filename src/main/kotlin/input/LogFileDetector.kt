package input

import java.io.File
import java.util.zip.ZipFile

enum class LogFileType {
    XES,        // Czysty XML
    OCEL_JSON,  // JSON (na przyszłość)
    ZIP,        // Archiwum .zip (może zawierać wiele plików)
    GZ,         // Pojedynczy plik skompresowany GZIP (najczęstszy format dużych logów)
    UNKNOWN
}

object LogFileDetector {

    fun detect(file: File): LogFileType {
        if (!file.exists()) return LogFileType.UNKNOWN

        // Prosta detekcja po rozszerzeniu.
        // W produkcyjnym kodzie można by sprawdzać "Magic Bytes" (nagłówki pliku),
        // ale sprawdzanie rozszerzenia jest wystarczające w 99% przypadków.
        return when {
            file.extension.equals("xes", ignoreCase = true) -> LogFileType.XES

            file.extension.equals("json", ignoreCase = true) -> LogFileType.OCEL_JSON

            file.extension.equals("zip", ignoreCase = true) -> {
                // Dla ZIP-a możemy opcjonalnie sprawdzić, co jest w środku,
                // ale ostatecznie i tak zwracamy typ ZIP, żeby Importer wiedział jak to otworzyć.
                LogFileType.ZIP
            }

            // Obsługa .gz lub .xes.gz
            file.name.endsWith(".gz", ignoreCase = true) -> LogFileType.GZ

            else -> LogFileType.UNKNOWN
        }
    }

    // Metoda pomocnicza - sprawdza czy w ZIPie jest coś użytecznego.
    // Przydatne, żeby od razu odrzucić ZIPy ze zdjęciami z wakacji zamiast logów.
    private fun containsXesOrJson(file: File): Boolean {
        return try {
            ZipFile(file).use { zip ->
                val entries = zip.entries().toList()
                entries.any {
                    it.name.endsWith(".xes", ignoreCase = true) ||
                            it.name.endsWith(".json", ignoreCase = true)
                }
            }
        } catch (e: Exception) {
            false // Jeśli plik ZIP jest uszkodzony
        }
    }
}
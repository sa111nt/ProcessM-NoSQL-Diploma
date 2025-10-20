package input

import java.io.File
import java.util.zip.ZipFile

enum class LogFileType {
    XES,
    OCEL_JSON,
    ZIP,
    GZ,
    UNKNOWN
}

object LogFileDetector {

    fun detect(file: File): LogFileType {
        if (!file.exists()) return LogFileType.UNKNOWN

        return when {
            file.extension.equals("xes", ignoreCase = true) -> LogFileType.XES
            file.extension.equals("json", ignoreCase = true) -> LogFileType.OCEL_JSON
            file.extension.equals("zip", ignoreCase = true) -> {
                if (containsXesOrJson(file)) LogFileType.ZIP else LogFileType.ZIP
            }
            file.name.endsWith(".gz", true) -> LogFileType.GZ //TODO dodać obsługę GZ tak jak dla ZIP i dodac to do maina
            else -> LogFileType.UNKNOWN
        }
    }

    private fun containsXesOrJson(file: File): Boolean {
        ZipFile(file).use { zip ->
            val entries = zip.entries().toList()
            return entries.any { it.name.endsWith(".xes") || it.name.endsWith(".json") }
        }
    }
}
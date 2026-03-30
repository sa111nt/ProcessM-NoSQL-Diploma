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
            file.extension.equals("zip", ignoreCase = true) -> LogFileType.ZIP
            file.name.endsWith(".gz", ignoreCase = true) -> LogFileType.GZ
            else -> LogFileType.UNKNOWN
        }
    }

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
            false
        }
    }
}
package input

import java.io.File
import java.io.FileOutputStream
import java.util.zip.ZipFile

class ZipExtractor {

    fun extractLogFiles(zipFile: File): List<File> {
        val extractedFiles = mutableListOf<File>()
        val tempDir = createTempDir(prefix = "unzipped_logs_")

        ZipFile(zipFile).use { zip ->
            zip.entries().toList().forEach { entry ->
                if (!entry.isDirectory && (entry.name.endsWith(".xes") || entry.name.endsWith(".json"))) {
                    val outFile = File(tempDir, File(entry.name).name)
                    zip.getInputStream(entry).use { input ->
                        FileOutputStream(outFile).use { output ->
                            input.copyTo(output)
                        }
                    }
                    extractedFiles.add(outFile)
                }
            }
        }

        return extractedFiles
    }
}
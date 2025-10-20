package input

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.util.zip.GZIPInputStream

object GzipExtractor {
    fun extractGzipFile(gzFile: File, outputDir: File = gzFile.parentFile): File {
        val outputFileName = gzFile.name.removeSuffix(".gz")
        val outputFile = File(outputDir, outputFileName)

        GZIPInputStream(FileInputStream(gzFile)).use { gis ->
            FileOutputStream(outputFile).use { fos ->
                gis.copyTo(fos)
            }
        }

        return outputFile
    }
}
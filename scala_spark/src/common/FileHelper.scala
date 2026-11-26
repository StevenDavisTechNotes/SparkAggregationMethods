package common

import java.io.{ File, FileOutputStream, IOException, PrintWriter }

object FileHelper {

  // from https://stackoverflow.com/questions/25999255/delete-directory-recursively-in-scala
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new IOException(s"Unable to delete ${file.getAbsolutePath}")
  }

  /** Opens `filename` for appending, creating missing parent directories. */
  def openAppend(filename: String): PrintWriter = {
    val resultFile = new File(filename)
    Option(resultFile.getAbsoluteFile.getParentFile).foreach(_.mkdirs())
    new PrintWriter(new FileOutputStream(resultFile, true))
  }
}

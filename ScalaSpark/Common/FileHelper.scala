package Common

import java.io._

object FileHelper {
   
  // from https://stackoverflow.com/questions/25999255/delete-directory-recursively-in-scala
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")   
  }

  def openAppend(filename: String): PrintWriter = {
    val resultFile = new File(filename)
    var pw: PrintWriter = null;
    if(resultFile.exists()) {
      pw = new PrintWriter(new FileOutputStream(resultFile, true))
    } else {
      pw = new PrintWriter(filename)
    }
    pw
  }

}
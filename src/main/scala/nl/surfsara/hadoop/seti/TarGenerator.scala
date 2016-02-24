package nl.surfsara.hadoop.seti

import java.io.{File, FileInputStream, FilenameFilter, OutputStream}

import grizzled.slf4j.Logging
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.commons.io.IOUtils

class TarGenerator(val destOutputStream: OutputStream) extends Logging {
  val tarGzOut = new TarArchiveOutputStream(destOutputStream)

  def closeTar() {
    //    tarGzOut.closeArchiveEntry()
    tarGzOut.close()
  }

  def addFileToTar(path: String) {
    addFileToTar(path, "", true, null, 0)
  }

  def addFileToTar(path: String, recursive: Boolean) {
    addFileToTar(path, "", recursive, null, 0)
  }

  def addFileToTar(path: String, recursive: Boolean, filter: FilenameFilter) {
    addFileToTar(path, "", recursive, filter, 0)
  }

  def addFileToTar(path: String, base: String, recursive: Boolean, filter: FilenameFilter, currentDepth: Int) {
    val nextDepth = currentDepth + 1
    debug("Tarring path " + path + ", recursive: " + recursive + ", current depth: " + currentDepth)
    val f = new File(path)
    val entryName = base + f.getName
    val tarEntry = new TarArchiveEntry(f, entryName)
    tarGzOut.putArchiveEntry(tarEntry)
    if (f.isFile) {
      IOUtils.copyLarge(new FileInputStream(f), tarGzOut)
      tarGzOut.closeArchiveEntry()
    } else {
      tarGzOut.closeArchiveEntry()
      if (nextDepth < 2 || recursive) {
        f.listFiles(filter).foreach(f => {
          addFileToTar(f.getAbsolutePath, entryName + "/", recursive, filter, nextDepth)
        })
      }
    }
  }
}

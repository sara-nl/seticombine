package nl.surfsara.hadoop.seti

import java.io.{File, FilenameFilter}

class FilenameExtensionFilter(extensions: Seq[String]) extends FilenameFilter {
  override def accept(dir: File, name: String): Boolean = {
    if (name == null || dir == null)
      false
    else
      extensions.filterNot(_ == null).map(name.endsWith(_)).fold(false) { (x, y) => x || y }
  }
}

object FilenameExtensionFilter {
  def apply(extensions: String*): FilenameExtensionFilter = {
    new FilenameExtensionFilter(extensions)
  }
}
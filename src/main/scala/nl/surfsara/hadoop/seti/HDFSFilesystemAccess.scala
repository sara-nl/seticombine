package nl.surfsara.hadoop.seti

import java.io.OutputStream
import java.security.PrivilegedExceptionAction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation

class HDFSFilesystemAccess(val hconf: Configuration, val ugi: UserGroupInformation) {
  val fs: FileSystem = FileSystem.get(hconf)

  def getOutputStreamForPath(path: String): OutputStream = {
    ugi.doAs(new PrivilegedExceptionAction[OutputStream] {
      override def run(): OutputStream = {
        fs.create(new Path(path))
      }
    })
  }
}

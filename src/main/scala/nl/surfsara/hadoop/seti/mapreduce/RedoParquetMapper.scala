package nl.surfsara.hadoop.seti.mapreduce

import java.io.{File, FileOutputStream, OutputStream}
import java.util.UUID
import java.util.zip.GZIPInputStream

import grizzled.slf4j.Logging
import nl.surfsara.hadoop.seti.{DatParser, FilenameExtensionFilter}
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class RedoParquetMapper extends Mapper[LongWritable, Text, NullWritable, Text] with Logging {

  def unTarGZStream(hdfsIn: FSDataInputStream, destinationDir: File) = {
    val in: TarArchiveInputStream = new TarArchiveInputStream(new GZIPInputStream(hdfsIn))
    var entry: TarArchiveEntry = in.getNextTarEntry
    while (entry != null) {
      if (entry.isDirectory) {
        entry = in.getNextTarEntry
      }
      val curfile: File = new File(destinationDir, entry.getName)
      val parent: File = curfile.getParentFile
      if (!parent.exists) {
        parent.mkdirs
      }
      val out: OutputStream = new FileOutputStream(curfile)
      IOUtils.copyLarge(in, out)
      out.close
      entry = in.getNextTarEntry
    }
    in.close
  }

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, NullWritable, Text]#Context): Unit = {
    val localDir: String = context.getConfiguration.get("job.local.dir") + "/" + UUID.randomUUID.toString + "/"
    val hdfsPath: Path = new Path(value.toString)
    val fs: FileSystem = FileSystem.get(context.getConfiguration)
    val hdfsIn: FSDataInputStream = fs.open(hdfsPath)
    unTarGZStream(hdfsIn, new File(localDir))
    hdfsIn.close

    val inDirectory = new File(localDir, "products/")
    val jsonIterator = inDirectory.listFiles(FilenameExtensionFilter(".dat")).map(DatParser.processDatFile(_))
    for (jsonList <- jsonIterator) {
      for (json <- jsonList) {
        debug("Writing json: \n " + json.toString(2))
        context.write(NullWritable.get, new Text(json.toString))
      }
    }
    val localDirs: File = new File(localDir)
    localDirs.delete
  }
}

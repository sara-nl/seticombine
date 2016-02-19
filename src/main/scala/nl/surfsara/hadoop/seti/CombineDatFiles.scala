package nl.surfsara.hadoop.seti

import java.io._
import java.security.PrivilegedExceptionAction

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject

import scala.io.Source

object CombineDatFiles extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val mnemonic: String = args(0)
    // TODO make type declarations consistent
    // TODO move to separate functions
    // P1 : parse dat files, convert to JSON, load into Spark and store as Parquet
    val inDirectory: File = new File(args(1))
    val outFile: File = new File(inDirectory.getAbsolutePath + "/" + mnemonic + ".json")
    val hdfsDir = args(2)
    val bw = new BufferedWriter(new FileWriter(outFile))
    inDirectory.listFiles(
      new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          name.endsWith(".dat")
        }
      }).foreach(processFile(_, bw))

    bw.flush()
    bw.close()

    val conf = new SparkConf().setAppName("SETI dat files 2 parquet")
    val sc = new SparkContext(conf)
    val sqlCtxt = new SQLContext(sc)

    val df = sqlCtxt.read.json("file://" + outFile.getAbsolutePath)
    df.printSchema()
    df.write.parquet(hdfsDir + "/" + mnemonic + "/parquet")

    // TODO move to separate functions
    // P2 : tar.gz all dat and log files and put to HDFS
    val sparkHadoopUtil: SparkHadoopUtil = SparkHadoopUtil.get

    val hconf = sparkHadoopUtil.newConfiguration(sc.getConf)
    val fileSystem: FileSystem = FileSystem.get(hconf)

    val ugi = UserGroupInformation.getCurrentUser
    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      def run: Unit = {
        // Copy out .fil file
        val gzPath: Path = new Path(hdfsDir + "/" + mnemonic + "/" + mnemonic + "_HighRes.fil.gz")
        val hOut = new GzipCompressorOutputStream(fileSystem.create(gzPath))
        IOUtils.copyLarge(new FileInputStream(new File(inDirectory.getAbsolutePath + "/" + mnemonic + "_HighRes.fil")), hOut)
        hOut.flush()
        hOut.close()

        // Copy out logs and dats
        val tarGzPath: Path = new Path(hdfsDir + "/" + mnemonic + "/" + mnemonic + "_datslogs.tar.gz")
        val hdfsOut = fileSystem.create(tarGzPath)
        val tarGzOut = new TarArchiveOutputStream(new GzipCompressorOutputStream(new BufferedOutputStream(hdfsOut)))
        addFileToTarGz(tarGzOut, inDirectory.getAbsolutePath, "")
        tarGzOut.flush()
        tarGzOut.close()
      }

      def addFileToTarGz(tarGzOut: TarArchiveOutputStream, path: String, base: String) {
        val f = new File(path)
        val entryName = base + f.getName
        val tarEntry = new TarArchiveEntry(f, entryName)
        tarGzOut.putArchiveEntry(tarEntry)
        logger.info("Tarring path " + path)
        if (f.isFile) {
          IOUtils.copyLarge(new FileInputStream(f), tarGzOut)
          tarGzOut.closeArchiveEntry()
        } else {
          tarGzOut.closeArchiveEntry()
          val children = f.listFiles(new FilenameFilter {
            override def accept(dir: File, name: String): Boolean = {
              name.endsWith(".dat") || name.endsWith(".log")
            }
          })
          for (child <- children) {
            addFileToTarGz(tarGzOut, child.getAbsolutePath, entryName + "/")
          }
        }
      }
    })

  }

  // TODO rename functions
  def processFile(inFile: File, bw: BufferedWriter) {
    logger.debug("Processing: " + inFile)
    val source = Source.fromFile(inFile)
    val sourceLines = source.getLines().filterNot(
      line => line.startsWith("---") || line.contains("candidates") || line.contains("Top Hit #")
    )

    val headerKeys: List[String] = List("fid", "mjd", "ra", "dec", "deltat", "deltaf", "doppler")
    val skipTokens: Set[String] = Set("File", "ID:", "MJD:", "RA:", "DEC:", "DELTAT:", "DELTAF(Hz):", "DOPPLER:")
    val headerVals: List[String] = sourceLines.take(2).flatMap(_.split("\\s+")).toList.filterNot(
      token => skipTokens.contains(token)
    )

    val header = headerKeys.zip(headerVals)

    val valueKeys = List("hitnum", "drift_rate", "snr", "uncorrected_freq", "corrected_freq", "index", "freq_start", "freq_end")
    val headerAndVals = sourceLines.map(_.split("\\s+")).map(header ++ valueKeys.zip(_)).map(
      tupleList => tupleList.map({
        case ("fid", v) => ("fid", v)
        case ("hitnum", v) => ("hitnum", v)
        case (k, v) => (k, v.toDouble)
      }))

    val jsonList = headerAndVals.map(list => toJSON(list))
    for (json <- jsonList) {
      bw.write(json.toString() + "\n")
    }
    source.close()
  }

  def toJSON(list: List[(String, Any)]): JSONObject = {
    val res = new JSONObject()
    list.map({ case (k, v) => res.put(k, v) })
    logger.debug(res.toString(2))
    res
  }

}

package nl.surfsara.hadoop.seti

import java.io._

import grizzled.slf4j.Logging
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.apache.commons.io.IOUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject

import scala.io.{BufferedSource, Source}

object Main extends Logging {

  // Singleton inner object for Spark and Hadoop accesses
  object SparkHadoopCtxt {
    val conf = new SparkConf().setAppName("SETI files 2 HDFS")
    val sc = new SparkContext(conf)
    val hconf = SparkHadoopUtil.get.newConfiguration(sc.getConf)
    val ugi = UserGroupInformation.getCurrentUser
    val hdfsAccess = new HDFSFilesystemAccess(hconf, ugi)
  }

  def main(args: Array[String]): Unit = {
    info("Starting seticombine...")
    info("Arguments: " + args.deep.mkString(" "))
    if (args.length != 3) {
      info("Not enough arguments..")
      System.exit(1)
    } else {
      val mnemonic = args(0)
      val inDirectory = new File(args(1))
      val hdfsBaseDir = args(2)

      try {
        val jsonFile = convertDats2Json(mnemonic, inDirectory)
        save2Parquet(jsonFile, hdfsBaseDir, mnemonic)
        datsLogs2HDFS(inDirectory, hdfsBaseDir, mnemonic)
        fil2HDFS(inDirectory, hdfsBaseDir, mnemonic)
      } catch {
        case t: Throwable => {
          error(t.getMessage, t)
          System.exit(1)
        }
      }
    }
  }

  def convertDats2Json(mnemonic: String, inDirectory: File): File = {
    val outFile = new File(inDirectory.getAbsolutePath + "/" + mnemonic + ".json")
    val bw = new BufferedWriter(new FileWriter(outFile))
    val jsonIterator = inDirectory.listFiles(FilenameExtensionFilter(".dat")).map(processDatFile(_))
    for (jsonList <- jsonIterator) {
      for (json <- jsonList) {
        debug("Writing json: \n " + json.toString(2))
        bw.write(json.toString() + "\n")
      }
    }
    bw.flush()
    bw.close()
    outFile
  }

  def processDatFile(inFile: File): List[JSONObject] = {
    debug("Processing: " + inFile)
    val source = Source.fromFile(inFile)
    val sourceLines = source.getLines().filterNot(
      line => line.startsWith("---") || line.contains("candidates") || line.contains("Top Hit #")
    )

    // Sample file: this top part will become header
    //  File ID: L241833_SAP001_B001_P002_HighRes_0
    //  Source: GJ411   MJD: 56889.552083333343 RA: 59.80000000 DEC: 6.60000000 DELTAT:   0.671089      DELTAF(Hz):   1.497456
    //  --------------------------
    //  RA_tile:2.899020        DEC_tile:0.693739       RA_TAB:2.849232 DEC_TAB:0.588790
    //  Pulsar_run:0    Pulsar_found:0  Pulsar_DM:0.000000      Pulsar_SNR:0.000000
    //  RFI_level:0.168717      N_stations:22
    //  --------------------------
    //  N_candidates: 7416

    val numHeaderLines = 5 // Of the file above there are five lines we are interested in
    val headerKeys: List[String] = List("fid", "source", "mjd", "ra", "dec", "deltat", "deltaf", "ra_tile", "dec_tile", "ra_tab", "dec_tab", "pulsar_run", "pulsar_found",
        "pulsar_dm", "pulsar_snr", "rfi_level", "n_stations", "n_candidates")
    val skipTokens: Set[String] = Set("File", "ID", "MJD", "RA", "DEC", "DELTAT", "DELTAF(Hz)", "DOPPLER", "Source",
      "RA_tile", "DEC_tile", "RA_TAB", "DEC_TAB", "Pulsar_run", "Pulsar_found", "Pulsar_DM", "Pulsar_SNR", "RFI_level", "N_stations", "N_candidates")
    val headerVals: List[String] = sourceLines.take(numHeaderLines).map(_.replaceAll(":", " ")).flatMap(_.split("\\s+")).toList.filterNot(
      token => skipTokens.contains(token)
    )

    val header = headerKeys.zip(headerVals)

    // Sample file: the rest will be prefixed with the header
    //  N_candidates: 7416
    //  --------------------------
    //  Top Hit #       Drift Rate      SNR     Uncorrected Frequency   Corrected Frequency     Index   freq_start      freq_end
    //  --------------------------
    //  001       0.027060        7.459188          123.988742      123.988742  72081       123.985675      123.991807
    //  002      -5.210030        5.126469          124.021152      124.021152  93724       124.018085      124.024217

    val valueKeys = List("hitnum", "drift_rate", "snr", "uncorrected_freq", "corrected_freq", "index", "freq_start", "freq_end")
    val headerAndVals = sourceLines.map(_.split("\\s+")).map(header ++ valueKeys.zip(_)).map(
      tupleList => tupleList.map({
        case ("fid", v) => ("fid", v)
        case ("source", v) => ("source", v)
        case ("hitnum", v) => ("hitnum", v)
        case (k, v) => (k, v.toDouble)
      }))

    val jsonList = headerAndVals.map(list => toJSON(list)).toList
    source.close()
    jsonList
  }

  def toJSON(list: List[(String, Any)]): JSONObject = {
    val res = new JSONObject()
    list.map({ case (k, v) => res.put(k, v) })
    debug(res.toString(2))
    res
  }

  def save2Parquet(jsonFile: File, hdfsBaseDir: String, mnemonic: String) {
    val sqlCtxt = new SQLContext(SparkHadoopCtxt.sc)

    val schema = StructType(Array(
      StructField("fid", StringType, false),
      StructField("freq_start", DoubleType, false),
      StructField("dec", DoubleType, false),
      StructField("ra_tile", DoubleType, false),
      StructField("pulsar_run", IntegerType, false),
      StructField("deltaf", DoubleType, false),
      StructField("ra_tab", DoubleType, false),
      StructField("source", StringType, false),
      StructField("corrected_freq", DoubleType, false),
      StructField("pulsar_dm", DoubleType, false),
      StructField("uncorrected_freq", DoubleType, false),
      StructField("drift_rate", DoubleType, false),
      StructField("deltat", DoubleType, false),
      StructField("dec_tile", DoubleType, false),
      StructField("hitnum", StringType, false),
      StructField("mjd", DoubleType, false),
      StructField("rfi_level", DoubleType, false),
      StructField("freq_end", DoubleType, false),
      StructField("index", DoubleType, false),
      StructField("ra", DoubleType, false),
      StructField("dec_tab", DoubleType, false),
      StructField("snr", DoubleType, false),
      StructField("n_stations", IntegerType, false),
      StructField("pulsar_snr", DoubleType, false),
      StructField("pulsar_found", IntegerType, false)
    ))

    val df = sqlCtxt.read.schema(schema).json("file://" + jsonFile.getAbsolutePath)
    df.printSchema()
    df.write.parquet(hdfsBaseDir + "/" + mnemonic + "/parquet")
  }

  def datsLogs2HDFS(inDirectory: File, hdfsBaseDir: String, mnemonic: String) {
    val hdfsOut = SparkHadoopCtxt.hdfsAccess.getOutputStreamForPath(hdfsBaseDir + "/" + mnemonic + "/" + mnemonic + "_datslogs.tar.gz")
    val tarGzGen = new TarGenerator(new GzipCompressorOutputStream(new BufferedOutputStream(hdfsOut)))
    tarGzGen.addFileToTar(inDirectory.getAbsolutePath, true, FilenameExtensionFilter(".dat", ".log"))
    tarGzGen.closeTar()
  }

  def fil2HDFS(inDirectory: File, hdfsBaseDir: String, mnemonic: String) {
    val hdfsOut = SparkHadoopCtxt.hdfsAccess.getOutputStreamForPath(hdfsBaseDir + "/" + mnemonic + "/" + mnemonic + "_HighRes.fil.gz")
    val gzOut = new GzipCompressorOutputStream(hdfsOut)
    val fileList = inDirectory.listFiles(FilenameExtensionFilter("_HighRes.fil")).toList
    if (fileList.size == 1) {
      IOUtils.copyLarge(new FileInputStream(fileList(0)), hdfsOut)
      hdfsOut.flush()
      hdfsOut.close()
    } else if (fileList.size > 1) {
      throw new Exception("More than one .fil file found")
    } else {
      throw new Exception("No .fil file found")
    }
  }
}

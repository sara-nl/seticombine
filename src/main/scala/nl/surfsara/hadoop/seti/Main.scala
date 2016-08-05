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

import scala.io.Source
import scala.util.control.NonFatal

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
        datsLogs2HDFS(inDirectory, hdfsBaseDir, mnemonic)
        save2Parquet(jsonFile, hdfsBaseDir, mnemonic)
        fil2HDFS(inDirectory, hdfsBaseDir, mnemonic)
      } catch {
        case NonFatal(e) => {
          error(e.getMessage, e)
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
      line => line.startsWith("---") || line.contains("Top Hit #")
    )

    // Sample file: this top part will become header
    //    File ID: L241833_SAP001_B001_P002_HighRes_2
    //    Source: GJ411   MJD: 56889.552083333343 RA: 59.80000000 DEC: 6.60000000 DELTAT:   0.671089      DELTAF(Hz):   1.497456
    //    --------------------------
    //    RA_tile:2.899020        DEC_tile:0.693739       RA_beam:2.894352        DEC_beam:0.627793       RA_TAB:2.849232 DEC_TAB:0.588790
    //    Pulsar_run:0    Pulsar_found:0  Pulsar_DM:0.000000      Pulsar_SNR:0.000000
    //    RFI_level:0.000000      N_stations:22
    //    Mean_SEFD:0.0   psrflux_Sens:0.0
    //    --------------------------
    //    N_candidates: 63393

    val numHeaderLines = 7 // Of the file above there are six lines we are interested in
    val headerKeys: List[String] = List("fid", "source", "mjd", "ra", "dec", "deltat", "deltaf", "ra_tile", "dec_tile", "ra_beam", "dec_beam", "ra_tab", "dec_tab", "pulsar_run", "pulsar_found",
        "pulsar_dm", "pulsar_snr", "rfi_level", "n_stations", "mean_sefd", "psrflux_sens", "n_candidates")
    val skipTokens: Set[String] = Set("File", "ID", "MJD", "RA", "DEC", "DELTAT", "DELTAF(Hz)", "DOPPLER", "Source",
      "RA_tile", "DEC_tile", "RA_beam", "DEC_beam", "RA_TAB", "DEC_TAB", "Pulsar_run", "Pulsar_found", "Pulsar_DM", "Pulsar_SNR", "RFI_level", "N_stations", "Mean_SEFD", "psrflux_Sens", "N_candidates")
    val headerVals: List[String] = sourceLines.take(numHeaderLines).map(_.replaceAll(":", " ")).flatMap(_.split("\\s+")).toList.filterNot(
      token => skipTokens.contains(token)
    )

    val header = headerKeys.zip(headerVals)

    // Sample file: the rest will be prefixed with the header
    //    N_candidates: 63393
    //    --------------------------
    //    Top Hit #       Drift Rate      SNR     Uncorrected Frequency   Corrected Frequency     Index   freq_start      freq_end        SEFD    SEFD_freq
    //      --------------------------
    //    001      -0.004163        6.913224          124.295984      124.295984  6921        124.294451      124.297516  0.0           0.000000
    //    002      -0.004163       12.377030          124.299051      124.299051  8969        124.297518      124.300583  0.0           0.000000


    val valueKeys = List("hitnum", "drift_rate", "snr", "uncorrected_freq", "corrected_freq", "index", "freq_start", "freq_end", "sefd", "sefd_freq")
    val headerAndVals = sourceLines.map(_.split("\\s+")).map(header ++ valueKeys.zip(_)).map(
      tupleList => tupleList.map({
        case ("fid", v) => ("fid", v)
        case ("source", v) => ("source", v)
        case ("hitnum", v) => ("hitnum", v.toInt)
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

    // JSON Structure:
    //    "fid": "L241833_SAP001_B001_P002_HighRes_2",
    //    "freq_start": 124.303853,
    //    "dec": 6.6,
    //    "ra_tile": 2.89902,
    //    "pulsar_run": 0,
    //    "deltaf": 1.497456,
    //    "ra_tab": 2.849232,
    //    "ra_beam": 2.894352,
    //    "source": "GJ411",
    //    "corrected_freq": 124.305387,
    //    "pulsar_dm": 0,
    //    "uncorrected_freq": 124.305387,
    //    "drift_rate": 1.321762,
    //    "n_candidates": 63393,
    //    "mean_sefd": 0,
    //    "sefd_freq": 0,
    //    "deltat": 0.671089,
    //    "dec_tile": 0.693739,
    //    "psrflux_sens": 0,
    //    "hitnum": 3,
    //    "sefd": 0,
    //    "mjd": 56889.55208333334,
    //    "rfi_level": 0,
    //    "freq_end": 124.306919,
    //    "index": 13200,
    //    "ra": 59.8,
    //    "dec_beam": 0.627793,
    //    "dec_tab": 0.58879,
    //    "snr": 5.00737,
    //    "n_stations": 22,
    //    "pulsar_snr": 0,
    //    "pulsar_found": 0


    val schema = StructType(Array(
      StructField("fid", StringType, false),
      StructField("freq_start", DoubleType, false),
      StructField("dec", DoubleType, false),
      StructField("ra_tile", DoubleType, false),
      StructField("pulsar_run", IntegerType, false),
      StructField("deltaf", DoubleType, false),
      StructField("ra_tab", DoubleType, false),
      StructField("ra_beam", DoubleType, false),
      StructField("source", StringType, false),
      StructField("corrected_freq", DoubleType, false),
      StructField("pulsar_dm", DoubleType, false),
      StructField("uncorrected_freq", DoubleType, false),
      StructField("drift_rate", DoubleType, false),
      StructField("n_candidates", IntegerType, false),
      StructField("mean_sefd", DoubleType, false),
      StructField("sefd_freq", DoubleType, false),
      StructField("deltat", DoubleType, false),
      StructField("dec_tile", DoubleType, false),
      StructField("psrflux_sens", DoubleType, false),
      StructField("hitnum", IntegerType, false),
      StructField("sefd", DoubleType, false),
      StructField("mjd", DoubleType, false),
      StructField("rfi_level", DoubleType, false),
      StructField("freq_end", DoubleType, false),
      StructField("index", DoubleType, false),
      StructField("ra", DoubleType, false),
      StructField("dec_beam", DoubleType, false),
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

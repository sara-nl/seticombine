package nl.surfsara.hadoop.seti

import java.io.File

import grizzled.slf4j.Logging
import org.json.JSONObject

import scala.io.Source

object DatParser extends Logging {
  def processDatFile(inFile: File): List[JSONObject] = {
    debug("Processing: " + inFile)
    val source = Source.fromFile(inFile)
    val sourceLines = source.getLines().filterNot(
      line => line.startsWith("---") || line.contains("Top Hit #") || line == ""
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

    var patchedHeaderVals = List[String]()
    if (headerVals.length == 23) {
      // Assume space in Source value
      val sourceVals = headerVals.take(3)
      val correctedVals = List[String](sourceVals(0), sourceVals(1) + " " + sourceVals(2))
      patchedHeaderVals = correctedVals ++ headerVals.takeRight(20)
    } else {
      patchedHeaderVals = headerVals
    }
    val header = headerKeys.zip(patchedHeaderVals)

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
}

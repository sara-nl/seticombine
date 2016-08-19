package nl.surfsara.hadoop.seti

import java.io.File

import scala.io.Source

class Dat2JsonSpec extends UnitSpec {
  "A sample .dat file" should "be converted to json" in {
    val sample0 = new File("src/test/resources/sample_files/sample0.dat")
    val source = Source.fromFile(sample0)
    val sourcedLines = source.getLines()
    info("Source file contents: ")
    for (s <- sourcedLines) {
      info(s)
    }
    val jsonList = DatParser.processDatFile(sample0).toList
    info("Converted json list size: " + jsonList.size)
    info("Converted json objects: ")
    for (json <- jsonList) {
      info(json.toString(2))
    }
    assert(jsonList.size == 3)
    val hitnumDriftRate = jsonList.map(jsonObj => (jsonObj.get("hitnum"), jsonObj.get("drift_rate")))
    assert(hitnumDriftRate.contains((1, -0.004163)))
    assert(hitnumDriftRate.contains((2, -0.004163)))
    assert(hitnumDriftRate.contains((3, 1.321762)))
    assert(!hitnumDriftRate.contains((5, -0.214396)))
    source.close()
  }

  "A second, different, sample .dat file" should "be converted to json" in {
    val sample0 = new File("src/test/resources/sample_files/sample2.dat")
    val source = Source.fromFile(sample0)
    val sourcedLines = source.getLines()
    info("Source file contents: ")
    for (s <- sourcedLines) {
      info(s)
    }
    val jsonList = DatParser.processDatFile(sample0).toList
    info("Converted json list size: " + jsonList.size)
    info("Converted json objects: ")
    for (json <- jsonList) {
      info(json.toString(2))
    }
    assert(jsonList.size == 6)
    val hitnumDriftRate = jsonList.map(jsonObj => (jsonObj.get("hitnum"), jsonObj.get("drift_rate"))).take(3)
    assert(hitnumDriftRate.contains((1, 0.285167)))
    assert(hitnumDriftRate.contains((2, -0.526623)))
    assert(hitnumDriftRate.contains((3, -1.765124)))
    source.close()
  }

  "A third, different, sample .dat file" should "be converted to json" in {
    val sample0 = new File("src/test/resources/sample_files/sample3.dat")
    val source = Source.fromFile(sample0)
    val sourcedLines = source.getLines()
    info("Source file contents: ")
    for (s <- sourcedLines) {
      info(s)
    }
    val jsonList = DatParser.processDatFile(sample0).toList
    info("Converted json list size: " + jsonList.size)
    info("Converted json objects: ")
    for (json <- jsonList) {
      info(json.toString(2))
    }
    assert(jsonList.size == 3)
    val hitnumDriftRate = jsonList.map(jsonObj => (jsonObj.get("hitnum"), jsonObj.get("drift_rate")))
    assert(hitnumDriftRate.contains((1, -0.004163)))
    assert(hitnumDriftRate.contains((2, -0.004163)))
    assert(hitnumDriftRate.contains((3, 1.321762)))
    assert(!hitnumDriftRate.contains((5, -0.214396)))
    source.close()
  }

  "A different sample .dat file" should "not be converted to json" in {
    val sample1 = new File("src/test/resources/sample_files/sample1.dat")
    val source = Source.fromFile(sample1)
    val sourced = source.getLines()
    info("Source file contents: ")
    for (s <- sourced) {
      info(s)
    }
    val jsonList = DatParser.processDatFile(sample1).toList
    info("Converted json list size: " + jsonList.size)
    assert(jsonList.size == 0)
    source.close()
  }

}

package nl.surfsara.hadoop.seti

import java.io.File

class FilenameExtensionFilterSpec extends UnitSpec {
  val fileFilter = FilenameExtensionFilter(".dat", ".log", ".fil")
  val dir = new File("/tmp")

  "This FilenameExtensionFilter" should "accept all filenames ending in \".log\",\".dat\",\".fil\"" in {
    assert(fileFilter.accept(dir, "test.log"))
    assert(fileFilter.accept(dir, "test.fil"))
    assert(fileFilter.accept(dir, "test.dat"))
  }

  it should "not accept filenames ending in anything else than \".log\",\".dat\",\".fil\"" in {
    assert(!fileFilter.accept(dir, "test.something.else"))
    assert(!fileFilter.accept(dir, ""))
  }

  it should "not accept null filename or directory arguments" in {
    assert(!fileFilter.accept(dir, null))
    assert(!fileFilter.accept(null, "test"))
    assert(!fileFilter.accept(null, null))
  }

  "A FilenameExtensionFilter created with no or null args" should "accept nothing" in {
    var failFilter = FilenameExtensionFilter()
    assert(!failFilter.accept(dir, "test.log"))
    assert(!failFilter.accept(dir, "test.fil"))
    assert(!failFilter.accept(dir, "test.dat"))

    failFilter = FilenameExtensionFilter(null)
    assert(!failFilter.accept(dir, "test.log"))
    assert(!failFilter.accept(dir, "test.fil"))
    assert(!failFilter.accept(dir, "test.dat"))
  }
}


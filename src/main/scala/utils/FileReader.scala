class FileReader {
  def neighborReader(inputFile: String) :DataFrame = {
    val linesReadCounter = new AtomicLong(0L)
    Source.fromFile(inputFile)
          .getLines()
          .foreach(line => {
      val linesRead = linesReadCounter.incrementAndGet()
      if (linesRead % 1000 == 0) Console.println("%d lines read".format(linesRead))
      val Array(srcPmid, relPmid, score, linkName) = line
        .split("\t")
          })
  }
}

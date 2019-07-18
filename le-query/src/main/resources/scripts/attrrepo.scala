import org.apache.spark.storage.StorageLevel

// read config
val tableMap = mapper.convertValue[Map[String, String]](lattice.params.get("TABLE_MAP"))
val tableFmt = mapper.convertValue[Map[String, String]](lattice.params.get("TABLE_FORMAT"))
val storageLevel: Option[StorageLevel] =
  if (lattice.params.hasNonNull("STORAGE_LEVEL")) {
    Some(StorageLevel.fromString(lattice.params.get("STORAGE_LEVEL").asText()))
  } else {
    None
  }

// computation
for ((tbl, path) <- tableMap) {
  val fmt = tableFmt(tbl).toLowerCase
  val read = spark.read.format(fmt).load("hdfs://" + path)
  storageLevel match {
    case Some(lvl) =>
      read.persist(lvl).createOrReplaceTempView(tbl)
    case _ =>
      read.createOrReplaceTempView(tbl)
  }
}

// write output
lattice.outputStr = "Loaded tables: " + mapper.writeValueAsString(tableMap)

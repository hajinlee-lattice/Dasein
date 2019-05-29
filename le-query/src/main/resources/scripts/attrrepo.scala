import org.apache.spark.storage.StorageLevel

// read config
val tableMap = mapper.convertValue[Map[String, String]](lattice.params.get("TABLE_MAP"))
val tableFmt = mapper.convertValue[Map[String, String]](lattice.params.get("TABLE_FORMAT"))
val persistRawTables: Boolean = lattice.params.get("PERSIST_RAW_TABLES").asBoolean()

// computation
for ((tbl, path) <- tableMap) {
  val fmt = tableFmt(tbl).toLowerCase
  val read = spark.read.format(fmt).load("hdfs://" + path)
  if (persistRawTables) {
    read.persist(StorageLevel.MEMORY_AND_DISK_SER).createOrReplaceTempView(tbl)
  } else {
    read.createOrReplaceTempView(tbl)
  }
}

// write output
lattice.outputStr = "Loaded tables: " + mapper.writeValueAsString(tableMap)

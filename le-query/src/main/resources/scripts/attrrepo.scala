import org.apache.spark.storage.StorageLevel

// read config
val tableMap = mapper.convertValue[Map[String, String]](lattice.params.get("TABLE_MAP"))
val persistRawTables: Boolean = lattice.params.get("PERSIST_RAW_TABLES").asBoolean()

// computation
for ((tbl, path) <- tableMap) {
  val read = spark.read.format("avro").load("hdfs://" + path)
  if (persistRawTables) {
    read.persist(StorageLevel.MEMORY_AND_DISK_SER).createOrReplaceTempView(tbl)
  } else {
    read.createOrReplaceTempView(tbl)
  }
}

// write output
lattice.outputStr = "Loaded tables: " + mapper.writeValueAsString(tableMap)

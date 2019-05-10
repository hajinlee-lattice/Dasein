import org.apache.spark.storage.StorageLevel

// read config
val tableMap = mapper.convertValue[Map[String, String]](lattice.params.get("TABLE_MAP"))
val persistOnDisk: Boolean = lattice.params.get("PERSIST_ON_DISK").asBoolean()

// computation
for ((tbl, path) <- tableMap) {
  val read = spark.read.format("avro").load("hdfs://" + path)
  if (persistOnDisk) {
    read.persist(StorageLevel.DISK_ONLY).createOrReplaceTempView(tbl)
  } else {
    read.createOrReplaceTempView(tbl)
  }
}

// write output
lattice.outputStr = "Loaded tables: " + mapper.writeValueAsString(tableMap)

// read config
val tableMap = mapper.convertValue[Map[String, String]](lattice.params.get("TABLE_MAP"))

// computation
for ((tbl, path) <- tableMap) {
  spark.read.format("avro").load("hdfs://" + path).createOrReplaceTempView(tbl)
}

// write output
lattice.outputStr = "Loaded tables: " + mapper.writeValueAsString(tableMap)

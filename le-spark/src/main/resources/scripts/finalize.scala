//import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
//import org.apache.spark.sql.DataFrame
//
//val mapper = new ObjectMapper() with ScalaObjectMapper
//mapper.registerModule(DefaultScalaModule)
//val scriptTargets = List[JsonNode]()
//val scriptOutput = scala.collection.mutable.Map[Integer, DataFrame]()

val finalTargets: List[JsonNode] = scriptTargets.zipWithIndex map {t => {
  val tgt = t._1
  val idx = t._2
  if (scriptOutput.contains(idx)) {
    val df = scriptOutput.getOrElse(idx, null)
    val path = tgt.get("Path").asText()
    df.write.format("com.databricks.spark.avro").save(path)
    val json = mapper.createObjectNode()
    json.put("StorageType", "Hdfs")
    json.put("Path", path)
    json.put("Count", df.count())
    json
  } else {
    throw new RuntimeException(s"Can not find $idx-th output dataframe")
  }
}}

println("----- BEGIN SCRIPT OUTPUT -----")
println(mapper.writeValueAsString(finalTargets))

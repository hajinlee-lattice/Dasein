//import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
//import org.apache.spark.sql.DataFrame
//val mapper = new ObjectMapper() with ScalaObjectMapper
//mapper.registerModule(DefaultScalaModule)
//class LatticeContext(val input: List[DataFrame], val params: JsonNode, val targets: List[JsonNode]) {
//  var output: List[DataFrame] = List[DataFrame]()
//}
//val lattice: LatticeContext = new LatticeContext(Nil, null, List[JsonNode]())

val targets = lattice.targets
val output = lattice.output

if (targets.length != output.length) {
  throw new IllegalArgumentException(s"${targets.length} targets are declared " //
    + s"but ${output.length} outputs are generated!")
}

val finalTargets: List[JsonNode] = targets.zip(output).map { t =>
  val tgt = t._1
  val df = t._2
  val path = tgt.get("Path").asText()
  df.write.format("avro").save(path)
  val df2 = spark.read.format("avro").load(path)
  val json = mapper.createObjectNode()
  json.put("StorageType", "Hdfs")
  json.put("Path", path)
  json.put("Count", df2.count())
  json
}

println("----- BEGIN SCRIPT OUTPUT -----")
println(mapper.writeValueAsString(finalTargets))


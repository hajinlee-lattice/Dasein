//import com.fasterxml.jackson.databind.node.ObjectNode
//import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
//import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
//import scala.collection.JavaConverters._
//import java.util.stream.Collectors
//import org.apache.spark.sql.DataFrame
//val mapper = new ObjectMapper() with ScalaObjectMapper
//mapper.registerModule(DefaultScalaModule)
//
//class LatticeContext(val input: List[DataFrame], val params: JsonNode, val targets: List[ObjectNode]) {
//  var output: List[DataFrame] = List[DataFrame]()
//}
//val lattice: LatticeContext = new LatticeContext(Nil, null, List[ObjectNode]())

var targets = lattice.targets
var output = lattice.output

if (targets.length != output.length) {
  throw new IllegalArgumentException(s"${targets.length} targets are declared " //
    + s"but ${output.length} outputs are generated!")
}

var finalTargets: List[JsonNode] = targets.zip(output).par.map { t =>
  val tgt = t._1
  val df = t._2
  val path = tgt.get("Path").asText()
  val fmt = if (tgt.get("DataFormat") != null) tgt.get("DataFormat").asText().toLowerCase() else "avro"
  val partitionKeys = if (tgt.get("PartitionKeys") == null) List() //
  else tgt.get("PartitionKeys").elements.asScala.map(_.asText()).toList
  if (partitionKeys.isEmpty) {
    if (df.rdd.getNumPartitions > 200) {
      df.repartition(200).write.format(fmt).save(path)
    } else {
      df.write.format(fmt).save(path)
    }
  } else {
    df.write.partitionBy(partitionKeys: _*).format(fmt).save(path)
  }
  val json = mapper.createObjectNode()
  json.put("StorageType", "Hdfs")
  json.put("Path", path)
  if (fmt.equals("csv")) {
    json.put("Count", df.count())
  } else {
    val df2 = spark.read.format(fmt).load(path)
    json.put("Count", df2.count())
  }
  json.set("PartitionKeys", mapper.valueToTree(partitionKeys))
  if (!"avro".equals(fmt)) {
    json.put("DataFormat", tgt.get("DataFormat"))
  }
  json
}.toList

lattice.orphanViews.map { view => spark.catalog.dropTempView(view) }

var result = mapper.createObjectNode()
result.put("OutputStr", lattice.outputStr)
result.set("Output", mapper.valueToTree(finalTargets))

println("----- BEGIN SCRIPT OUTPUT -----")
println(mapper.writeValueAsString(result))

// clean up all variables to avoid memory leak
// https://gist.github.com/dragos/77b048c2baba93d36cd8

targets = null
output = null
finalTargets = null
result = null


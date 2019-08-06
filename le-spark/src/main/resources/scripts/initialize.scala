import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ObjectNode,ArrayNode,BooleanNode}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import scala.collection.JavaConverters._
import org.apache.spark.sql.{DataFrame, SparkSession}

class LatticeContext(val input: List[DataFrame], val params: JsonNode, val targets: List[ObjectNode]) {
  var output: List[DataFrame] = List[DataFrame]()
  var outputStr: String = ""
}

val mapper = new ObjectMapper() with ScalaObjectMapper
mapper.registerModule(DefaultScalaModule)

val spark = SparkSession.builder().appName("SparkSession").getOrCreate()

val checkpointDir = """{{CHECKPOINT_DIR}}"""

if (checkpointDir.length > 0) {
  spark.sparkContext.setCheckpointDir(checkpointDir)
  println("----- BEGIN SCRIPT OUTPUT -----")
  println(s"Checkpoint Dir: $checkpointDir")
  println("----- END SCRIPT OUTPUT -----")
}

val rawInput = mapper.readValue[List[JsonNode]]("""{{INPUT}}""")

def loadHdfsUnit(unit: JsonNode): DataFrame = {
  var path = unit.get("Path").asText()
  val fmt = if (unit.get("DataFormat") != null) unit.get("DataFormat").asText.toLowerCase else "avro"
  val partitionKeys = if (unit.get("PartitionKeys") == null) List() else unit.get("PartitionKeys").elements.asScala.map(_.asText()).toList

  if (!partitionKeys.isEmpty) {
    val suffix = "." + fmt
    if (!path.endsWith(suffix)) {
      if (path.endsWith("/")) {
        path += "*" + suffix
      } else {
        path += "/*" + suffix
      }
    }
  }
  println(s"load Path: $path")
  spark.read.format(fmt).load("hdfs://" + path)
}

val scriptInput = rawInput map { input => {
  val storage = input.get("StorageType").asText().toLowerCase()
  storage match {
    case "hdfs" => loadHdfsUnit(input)
    case _ => throw new UnsupportedOperationException(s"Unknown storage $storage")
  }
}}

println("----- BEGIN SCRIPT OUTPUT -----")
println(s"Input: $scriptInput")
println("----- END SCRIPT OUTPUT -----")

val scriptParams = mapper.readValue[JsonNode]("""{{PARAMS}}""")

println("----- BEGIN SCRIPT OUTPUT -----")
println(s"Params: $scriptParams")
println("----- END SCRIPT OUTPUT -----")

val scriptTargets = mapper.readValue[List[ObjectNode]]("""{{TARGETS}}""")

println("----- BEGIN SCRIPT OUTPUT -----")
println(s"Targets: $scriptTargets")
println("----- END SCRIPT OUTPUT -----")

val lattice: LatticeContext = new LatticeContext(scriptInput, scriptParams, scriptTargets)

def setPartitionTargets(index: Int, partitionKeys: Seq[String],lattice: LatticeContext): Unit = {
  val array = mapper.valueToTree[ArrayNode](partitionKeys, classOf[ArrayNode])
  lattice.targets(index).set("PartitionKeys", array)
}

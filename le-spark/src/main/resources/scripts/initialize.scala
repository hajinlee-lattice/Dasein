import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.{DataFrame, SparkSession}

class LatticeContext(val input: List[DataFrame], val params: JsonNode, val targets: List[JsonNode]) {
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
  val suffix = "." + fmt
  if (!path.endsWith(suffix)) {
    if (path.endsWith("/")) {
      path += "*" + suffix
    } else {
      path += "/*" + suffix
    }
  }
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

val scriptTargets = mapper.readValue[List[JsonNode]]("""{{TARGETS}}""")

println("----- BEGIN SCRIPT OUTPUT -----")
println(s"Targets: $scriptTargets")
println("----- END SCRIPT OUTPUT -----")

val lattice: LatticeContext = new LatticeContext(scriptInput, scriptParams, scriptTargets);

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.{DataFrame, SparkSession}

val mapper = new ObjectMapper() with ScalaObjectMapper
mapper.registerModule(DefaultScalaModule)

val scriptTargets = mapper.readValue[List[JsonNode]]("{{TARGETS}}")
val scriptOutput = scala.collection.mutable.Map[Integer, DataFrame]()

println("----- BEGIN SCRIPT OUTPUT -----")
println(s"Targets: $scriptTargets")
println("----- END SCRIPT OUTPUT -----")

val spark = SparkSession.builder().appName("SparkSession").getOrCreate()

val rawInput = mapper.readValue[List[JsonNode]]("{{INPUT}}")

def loadHdfsUnit(unit: JsonNode): DataFrame = {
  val path = unit.get("Path").asText()
  spark.read.format("com.databricks.spark.avro").load("hdfs://" + path)
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

val scriptParams = mapper.readValue[JsonNode]("{{PARAMS}}")

println("----- BEGIN SCRIPT OUTPUT -----")
println(s"Params: $scriptParams")
println("----- END SCRIPT OUTPUT -----")

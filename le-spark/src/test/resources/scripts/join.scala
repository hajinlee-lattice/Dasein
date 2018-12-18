//import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions._
//val mapper = new ObjectMapper() with ScalaObjectMapper
//mapper.registerModule(DefaultScalaModule)
//class LatticeContext(val input: List[DataFrame], val params: JsonNode, val targets: List[JsonNode]) {
//  var output: List[DataFrame] = List[DataFrame]()
//}
//val lattice: LatticeContext = new LatticeContext(Nil, null, List[JsonNode]())

// ============
// BEGIN SCRIPT
// ============

// read input
val table1: DataFrame = lattice.input(0)
val table2: DataFrame = lattice.input(1)

// read config
val joinKey = lattice.params.get("JOIN_KEY").asText()
val aggKey = lattice.params.get("AGG_KEY").asText()

// computation
val df = table1.join(table2, table1(joinKey) === table2(joinKey), "outer").groupBy(table2(joinKey))
val out1 = df.count().withColumnRenamed("count", "Cnt")
val out2 = df.agg(max(table1(aggKey)).as("Max1"), max(table2(aggKey)).as("Max2"))

// write output
lattice.output = out1::out2::Nil
lattice.outputStr = "This is my output!"

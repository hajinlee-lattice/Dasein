//import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
//import com.fasterxml.jackson.databind.node.{ObjectNode,ArrayNode,BooleanNode}
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
if(lattice.params.hasNonNull("Partition") && lattice.params.get("Partition").asBoolean()){
  setPartitionTargets(0, Seq("Field1","Field2","Field3","Field4","Field5"), lattice)
}

val result = lattice.input.head

// finish
lattice.output = result :: Nil
lattice.outputStr = "This is script output!"

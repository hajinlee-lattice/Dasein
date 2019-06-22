package com.latticeengines.spark.exposed.job.cdl

import com.latticeengines.domain.exposed.spark.cdl.JoinConfig
import com.latticeengines.spark.exposed.job.{AbstractSparkJob, LatticeContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import scala.collection.mutable.Map
import org.apache.spark.sql.types._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.io.StringWriter;

class JoinJob extends AbstractSparkJob[JoinConfig] {

  override def runJob(spark: SparkSession, lattice: LatticeContext[JoinConfig]): Unit = {
    val config: JoinConfig = lattice.config
    val joinKey: String = config.getJoinKey
	println(s"joinKey is: $joinKey");
	
	// read input
    val accountTable: DataFrame = lattice.input.head
    val contactTable: DataFrame = lattice.input(1)

	// manupulate contact table
	spark.udf.register("flatten", new Flatten)
	val f = new Flatten
	val result = contactTable.groupBy("Field1").agg(f(col("ID"), col("Field2")).as("result"))
	result.show()

    // join
    val df = accountTable.join(contactTable, joinKey::Nil, "left").groupBy(joinKey)
    val out1 = df.count().withColumnRenamed("count", "Cnt")
	
    // finish
    lattice.output = out1::Nil
    lattice.outputStr = "This is my recommendation!"
  }
  
  class Flatten extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(List(StructField("ID", StringType), StructField("Field2", StringType)))

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("list", ArrayType(DataTypes.createMapType(StringType, StringType))) :: Nil)
 //   override def bufferSchema: StructType = ArrayType(inputSchema(), true)

  // This is the output type of your aggregatation function.
  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = IndexedSeq[Map[String, String]]()
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//     val ele = Map(input.getAs[String](0) -> input.getAs[String](1))
    val ele = Map("ID" -> input.getAs[String](0), "Field2" -> input.getAs[String](1))
    val cur = buffer(0).asInstanceOf[IndexedSeq[Map[String, String]]]
    buffer(0) = cur :+ ele
      
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val seq1 = buffer1(0).asInstanceOf[IndexedSeq[Map[String, String]]]
    val seq2 = buffer2(0).asInstanceOf[IndexedSeq[Map[String, String]]]
    buffer1(0) = seq1 ++ seq2
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val writer: StringWriter = new StringWriter()
    mapper.writeValue(writer, buffer(0).asInstanceOf[IndexedSeq[Map[String, String]]])
    writer.toString()
  }
 }

}

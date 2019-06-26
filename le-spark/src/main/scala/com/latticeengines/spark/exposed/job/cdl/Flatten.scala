package com.latticeengines.spark.exposed.job.cdl

import org.apache.spark.sql._
import scala.collection.mutable.Map
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import java.io.StringWriter;

class Flatten(schema: StructType) extends UserDefinedAggregateFunction {

  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType = schema

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

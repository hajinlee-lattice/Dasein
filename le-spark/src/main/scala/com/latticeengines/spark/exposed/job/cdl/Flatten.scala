package com.latticeengines.spark.exposed.job.cdl

import java.io.StringWriter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import com.latticeengines.domain.exposed.metadata.InterfaceName
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable.Map;

class Flatten(schema: StructType, configuredColumns: Seq[String], sfdcContactId: String, userCustomerId: Boolean, isEntityMatch: Boolean) extends UserDefinedAggregateFunction {

  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType = schema

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("list", ArrayType(DataTypes.createMapType(StringType, StringType))) :: Nil)

  // This is the output type of your aggregatation function.
  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  var cols: Seq[String] = Seq.empty[String]
  var useConfiguredCols: Boolean = false
  var sfdcContactIdEmpty: Boolean = sfdcContactId == null

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = IndexedSeq[Map[String, String]]()
    cols = configuredColumns
    useConfiguredCols = if (cols.isEmpty) false else true
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var ele = Map.empty[String, String]
    if (useConfiguredCols) {
      for (col <- cols) {
        ele = ele + (col -> getInputValue(input, col))
      }
    } else {
      ele = Map(PlaymakerConstants.Email -> getInputValue(input, InterfaceName.Email.name()), //
        PlaymakerConstants.Address -> getInputValue(input, InterfaceName.Address_Street_1.name()), //
        PlaymakerConstants.Address2 -> getInputValue(input, InterfaceName.Address_Street_2.name()), //
        PlaymakerConstants.Phone -> getInputValue(input, InterfaceName.PhoneNumber.name()), //
        PlaymakerConstants.State -> getInputValue(input, InterfaceName.State.name()), //
        PlaymakerConstants.ZipCode -> getInputValue(input, InterfaceName.PostalCode.name()), //
        PlaymakerConstants.Country -> getInputValue(input, InterfaceName.Country.name()), //
        PlaymakerConstants.SfdcContactID -> (if (sfdcContactIdEmpty) getInputValue(input, getContactId(isEntityMatch)) else getInputValue(input, sfdcContactId)), //
        PlaymakerConstants.City -> getInputValue(input, InterfaceName.City.name()), //
        PlaymakerConstants.ContactID -> getInputValue(input, getContactId(userCustomerId)), //
        PlaymakerConstants.Name -> getInputValue(input, InterfaceName.ContactName.name()), //
        PlaymakerConstants.FirstName -> getInputValue(input, InterfaceName.FirstName.name()), //
        PlaymakerConstants.LastName -> getInputValue(input, InterfaceName.LastName.name()), //
        PlaymakerConstants.Title -> getInputValue(input, InterfaceName.Title.name()), //
        PlaymakerConstants.DoNotCall -> getInputValue(input, InterfaceName.DoNotCall.name()), //
        PlaymakerConstants.DoNotMail -> getInputValue(input, InterfaceName.DoNotMail.name()), //
        PlaymakerConstants.CreatedDate -> getInputValue(input, InterfaceName.CreatedDate.name()), //
        PlaymakerConstants.LastModifiedDate -> getInputValue(input, InterfaceName.LastModifiedDate.name()))

    }
    val cur = buffer(0).asInstanceOf[IndexedSeq[Map[String, String]]]
    buffer(0) = cur :+ ele
  }

  def getContactId(userCustomerId: Boolean): String = {
    if (userCustomerId) {
      InterfaceName.CustomerContactId.name
    } else {
      InterfaceName.ContactId.name
    }
  }

  private def getInputValue(input: Row, key: String): String = {
    try {
      Option(input.getAs[Any](inputSchema.fieldIndex(key))).map(_.toString).getOrElse(null)
    } catch {
      case e: IllegalArgumentException => return null
    }
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

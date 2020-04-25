package com.latticeengines.spark.util

import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook.DecodeStrategy
import org.apache.avro.util.Utf8
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConverters._

private[spark] object BitEncodeUtils {

  def decode(input: DataFrame, outputCols: Seq[String], codeBookLookup: Map[String, String], codeBooks: Map[String, BitCodeBook]): DataFrame = {
    val encodedAttrs: Seq[String] = codeBookLookup.values.toSeq.intersect(input.columns)
    val retainFields: Seq[StructField] = input.schema.filter(field => outputCols.contains(field.name))
      .filterNot(field => codeBookLookup.keySet.contains(field.name))
    val newFields: Seq[StructField] = getDecodedFields(outputCols, codeBookLookup, codeBooks)
    val outputSchema = StructType(retainFields ++ newFields)
    val attrPos: Map[String, Int] = outputSchema.zipWithIndex.map(t => (t._1.name, t._2)).toMap

    val selected: DataFrame = input.select(input.columns.intersect(outputCols ++ encodedAttrs).map(col): _*)
    val outputWidth: Int = attrPos.size

    selected.map(row => {
      val pairs: Map[String, Any] = row.toSeq.zip(row.schema.names).flatMap(t => {
        val value = t._1
        val field = t._2
        if (encodedAttrs.contains(field)) {
          val encodedStr = parseEncodedStr(value)
          val lst = decodeValue(field, encodedStr, codeBookLookup, codeBooks).toList
          lst
        } else {
          List((field, value))
        }
      }).toMap

      val values: Array[Any] = Array.ofDim[Any](outputWidth)
      pairs.foreach(t => {
        val value = t._2
        if (value != null && attrPos.contains(t._1)) {
          values.update(attrPos(t._1), value)
        }
      })

      Row.fromSeq(values)
    })(RowEncoder(outputSchema))
  }

  private def getDecodedFields(outputCols: Seq[String], codeBookLookup: Map[String, String], codeBooks: Map[String, BitCodeBook]): Seq[StructField] = {
    outputCols.filter(codeBookLookup.contains).map(attrName => {
      val bookName: String = codeBookLookup(attrName)
      codeBooks.get(bookName) match {
        case Some(codeBook: BitCodeBook) =>
          val strategy: DecodeStrategy = codeBook.getDecodeStrategy
          strategy match {
            case DecodeStrategy.BOOLEAN_YESNO => StructField(attrName, StringType, nullable = true)
            case DecodeStrategy.NUMERIC_INT => StructField(attrName, IntegerType, nullable = true)
            case DecodeStrategy.NUMERIC_UNSIGNED_INT => StructField(attrName, IntegerType, nullable = true)
            case DecodeStrategy.ENUM_STRING => StructField(attrName, StringType, nullable = true)
            case _ => throw new UnsupportedOperationException(s"Unknown decode strategy $strategy")
          }
        case _ => throw new IllegalArgumentException(s"Cannot find a code book named $bookName for encoded attribute $attrName")
      }
    })
  }

  private def parseEncodedStr(value: Any): String = {
    (if (value == null) {
      Some("")
    } else value match {
      case _: String =>
        Some(value.asInstanceOf[String])
      case _: Utf8 =>
        Some(value.toString)
      case _ =>
        Some("")
    }).get
  }

  private def decodeValue(encodedCol: String, encodedStr: String, codeBookLookup: Map[String, String], codeBooks: Map[String, BitCodeBook]): Map[String, Any] = {
    val codeBookOpt : Option[BitCodeBook] = codeBooks.get(encodedCol)
    codeBookOpt match {
      case Some(codeBook) =>
        val decodedCols = codeBookLookup.filter(t => encodedCol.equals(t._2)).keys.toList.asJava
        codeBook.decode(encodedStr, decodedCols).asScala.toMap
      case _ => throw new IllegalArgumentException(s"No bit code book for encoded attr $encodedCol")
    }
  }

}

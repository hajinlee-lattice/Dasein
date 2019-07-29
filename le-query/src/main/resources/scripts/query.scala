import java.io.ByteArrayInputStream
import java.nio.charset.Charset
import java.util.Base64
import java.util.zip.GZIPInputStream

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

val outputMode = lattice.params.get("OUTPUT_MODE").asText()

val decodeMapping: Map[String, Map[String, String]] =
  if (lattice.params.has("DECODE_MAPPING")) {
    mapper.treeToValue[Map[String, Map[String, String]]](lattice.params.get("DECODE_MAPPING"))
  } else {
    null
  }

def parseSqls(): List[List[String]] = {
  val sqlB64: String = lattice.params.get("SQLS").asText()
  val sqls = IOUtils.toString(new GZIPInputStream(new ByteArrayInputStream(Base64.getDecoder.decode(sqlB64))), //
    Charset.forName("UTF-8"))
  mapper.readValue[List[List[String]]](sqls)
}

def parseSql(): String = {
  val sqlB64: String = lattice.params.get("SQL").asText()
  IOUtils.toString(new GZIPInputStream(new ByteArrayInputStream(Base64.getDecoder.decode(sqlB64))), //
    Charset.forName("UTF-8"))
}

if (lattice.params.hasNonNull("SQLS")) {
  val pairs: List[List[String]] = parseSqls()
  pairs map {pair => {
    val name = pair.head
    val statement = pair(1)
    println("----- BEGIN SCRIPT OUTPUT -----")
    println(s"\nSQL Statement for $name:")
    println(statement)
    println("----- END SCRIPT OUTPUT -----")
    name
  }}
} else {
  val sql = parseSql()
  println("----- BEGIN SCRIPT OUTPUT -----")
  if (lattice.params.hasNonNull("VIEW_NAME")) {
    println("SQL Statement for " + lattice.params.get("VIEW_NAME").asText() + ":")
  } else {
    println("SQL Statement:")
  }
  println(sql)
  println("----- END SCRIPT OUTPUT -----")
}
// -----CELL BREAKER----

val sqlDF: DataFrame =
  if (lattice.params.hasNonNull("SQLS")) {
    val pairs: List[List[String]] = parseSqls()
    val finalStatment = pairs.foldLeft("")((_, pair) => {
      val name = pair.head
      val statement = pair(1)
      if (name != "final") {
        spark.sql(statement).createOrReplaceTempView(name)
      }
      statement
    })
    spark.sql(finalStatment)
  } else {
    val sql = parseSql()
    spark.sql(sql)
  }

def decode(df: DataFrame, decodeMapping: Map[String, Map[String, String]]): DataFrame = {
  if (decodeMapping == null) {
    df
  } else {
    decodeMapping.foldLeft(df)((df, t) => {
      val (attr, mapping) = t
      val decodeFunc: Long => String = longVal => mapping.getOrElse(longVal.toString, null)
      val decodeUdf = udf(decodeFunc)
      df.withColumn(attr, decodeUdf(col(attr)))
    })
  }
}

outputMode match {
  case "count" => lattice.outputStr = sqlDF.count().toString
  case "save" => lattice.output = decode(sqlDF, decodeMapping) :: Nil
  case "view" => {
    val viewName = lattice.params.get("VIEW_NAME").asText()
    sqlDF.createOrReplaceTempView(viewName)
    lattice.outputStr = viewName
  }
}

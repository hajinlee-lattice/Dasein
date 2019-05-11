import java.io.ByteArrayInputStream
import java.nio.charset.Charset
import java.util.Base64
import java.util.zip.GZIPInputStream

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

val sqlB64: String = lattice.params.get("SQL").asText()
val saveResult = lattice.params.get("SAVE").asBoolean()
val decodeMapping: Map[String, Map[String, String]] =
  if (lattice.params.has("DECODE_MAPPING")) {
    mapper.treeToValue[Map[String, Map[String, String]]](lattice.params.get("DECODE_MAPPING"))
  } else {
    null
  }

val sql = IOUtils.toString(new GZIPInputStream(new ByteArrayInputStream(Base64.getDecoder.decode(sqlB64))), //
  Charset.forName("UTF-8"))

println("----- BEGIN SCRIPT OUTPUT -----")
println("SQL Statement:")
println(sql)
// -----CELL BREAKER----

val sqlDF = spark.sql(sql)

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

if (saveResult) {
  lattice.output = decode(sqlDF, decodeMapping)::Nil
} else {
  lattice.outputStr = sqlDF.count().toString
}

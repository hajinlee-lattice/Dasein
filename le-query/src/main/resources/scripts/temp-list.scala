import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.zip.GZIPInputStream

import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

val valsStr: String = IOUtils.toString( //
  new GZIPInputStream(new ByteArrayInputStream(Base64.getDecoder.decode(longStrBldr.toString))), StandardCharsets.UTF_8)
val clzName = lattice.params.get("TYPE").asText

val vals: List[List[Any]] = if ("String".equalsIgnoreCase(clzName)) {
  mapper.readValue[List[List[String]]](valsStr)
} else if ("Integer".equalsIgnoreCase(clzName)) {
  mapper.readValue[List[List[Int]]](valsStr)
} else if ("Long".equalsIgnoreCase(clzName)) {
  mapper.readValue[List[List[Long]]](valsStr)
} else if ("Float".equalsIgnoreCase(clzName)) {
  mapper.readValue[List[List[Float]]](valsStr)
} else if ("Double".equalsIgnoreCase(clzName)) {
  mapper.readValue[List[List[Double]]](valsStr)
} else if ("Boolean".equalsIgnoreCase(clzName)) {
  mapper.readValue[List[List[Boolean]]](valsStr)
} else {
  mapper.readValue[List[List[String]]](valsStr)
}

val schema = StructType(List(
  if ("String".equalsIgnoreCase(clzName)) {
    StructField("value", StringType, nullable = false)
  } else if ("Integer".equalsIgnoreCase(clzName)) {
    StructField("value", IntegerType, nullable = false)
  } else if ("Long".equalsIgnoreCase(clzName)) {
    StructField("value", LongType, nullable = false)
  } else if ("Float".equalsIgnoreCase(clzName)) {
    StructField("value", FloatType, nullable = false)
  } else if ("Double".equalsIgnoreCase(clzName)) {
    StructField("value", DoubleType, nullable = false)
  } else if ("Boolean".equalsIgnoreCase(clzName)) {
    StructField("value", BooleanType, nullable = false)
  } else {
    StructField("value", StringType, nullable = false)
  }
))

val data: RDD[Row] = spark.sparkContext.parallelize(vals.map(l => Row(l.head)))
spark.createDataFrame(data, schema).createOrReplaceTempView(lattice.params.get("VIEW_NAME").asText)

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, min, when}

val viewList: List[String] = mapper.convertValue[List[String]](lattice.params.get("VIEW_LIST"))
val defaultBkt = lattice.params.get("DEFAULT_BUCKET").asText()

val viewDfs: List[DataFrame] = viewList map {viewName =>
  spark.sql(s"SELECT * FROM $viewName")
}

val result =
  if (viewDfs.length == 1) {
    viewDfs.head.withColumn("Rating", lit(defaultBkt));
  } else {
    val merged: DataFrame = viewDfs.reduce(_ union _)
    merged.groupBy("AccountId")
      .agg(min("Rating").alias("Rating"))
      .withColumn("Rating",
        when(col("Rating") === "Z", lit(defaultBkt)).otherwise(col("Rating")))
  }

lattice.output = result :: Nil

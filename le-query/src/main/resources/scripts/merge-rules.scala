import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, min, when}

val bktViews: List[String] = mapper.convertValue[List[String]](lattice.params.get("BKT_VIEWS"))
val tempViews: List[String] = mapper.convertValue[List[String]](lattice.params.get("TEMP_VIEWS"))
val defaultBkt = lattice.params.get("DEFAULT_BUCKET").asText()

val viewDfs: List[DataFrame] = bktViews map {viewName =>
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

lattice.orphanViews = tempViews
lattice.output = result :: Nil

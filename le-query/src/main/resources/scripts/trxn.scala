import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

spark.createDataFrame(
  spark.sparkContext.parallelize(-10 to 10000),
  StructType(List(StructField("PeriodId", IntegerType)))
).persist(StorageLevel.MEMORY_ONLY).createOrReplaceTempView("AllPeriodIds")

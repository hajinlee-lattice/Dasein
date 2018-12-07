//import org.apache.spark.sql.DataFrame
//val scriptInput = List[DataFrame]()
//val scriptOutput = Map[String, DataFrame]()

// read input
val table1: DataFrame = scriptInput(0)
val table2: DataFrame = scriptInput(1)

// read config
val joinKey = scriptParams.get("JOIN_KEY").asText()
val aggKey = scriptParams.get("AGG_KEY").asText()

// computation
val df = table1.join(table2, table1(joinKey) === table2(joinKey), "outer").groupBy(table2(joinKey))
val out1 = df.count().withColumnRenamed("count", "Cnt")
val out2 = df.agg(max(table1(aggKey)).as("Max1"), max(table2(aggKey)).as("Max2"))

// write output
scriptOutput(0) = out1
scriptOutput(1) = out2
println("----- BEGIN SCRIPT OUTPUT -----")
println("This is my output!")

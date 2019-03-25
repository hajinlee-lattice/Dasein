val sql = lattice.params.get("SQL").asText()
val saveResult = lattice.params.get("SAVE").asBoolean()

println("----- BEGIN SCRIPT OUTPUT -----")
println(s"SQL Statement:")
println(sql)
println("----- END SCRIPT OUTPUT -----")

val sqlDF = spark.sql(sql)

if (saveResult) {
  lattice.output = sqlDF::Nil
} else {
  lattice.outputStr = sqlDF.count().toString
}

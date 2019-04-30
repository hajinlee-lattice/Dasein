import java.io.ByteArrayInputStream
import java.nio.charset.Charset
import java.util.Base64
import java.util.zip.GZIPInputStream

import org.apache.commons.io.IOUtils

val sqlB64: String = lattice.params.get("SQL").asText()
val saveResult = lattice.params.get("SAVE").asBoolean()

val sql = IOUtils.toString(new GZIPInputStream(new ByteArrayInputStream(Base64.getDecoder.decode(sqlB64))), //
  Charset.forName("UTF-8"))

println("----- BEGIN SCRIPT OUTPUT -----")
println("SQL Statement:")
println(sql)
println("----- END SCRIPT OUTPUT -----")

val sqlDF = spark.sql(sql)

if (saveResult) {
  lattice.output = sqlDF::Nil
} else {
  lattice.outputStr = sqlDF.count().toString
}

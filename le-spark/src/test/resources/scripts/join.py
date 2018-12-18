# script_input = {}
# script_output = {}

import pyspark.sql.functions as sf

table1 = lattice.input[0]
table2 = lattice.input[1]

join_key = lattice.params["JOIN_KEY"]
agg_key = lattice.params["AGG_KEY"]

df = table1.join(table2, table1[join_key] == table2[join_key], "outer").groupBy(table2[join_key])
out1 = df.count().withColumnRenamed("count", "Cnt")
out2 = df.agg(sf.max(table1[agg_key]).alias("Max1"), sf.max(table2[agg_key]).alias("Max2"))

lattice.output = [out1, out2]
lattice.output_str = "This is my output!"

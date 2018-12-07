# script_input = {}
# script_output = {}

import pyspark.sql.functions as sf

table1 = script_input[0]
table2 = script_input[1]

join_key = script_params["JOIN_KEY"]
agg_key = script_params["AGG_KEY"]

df = table1.join(table2, table1[join_key] == table2[join_key], "outer").groupBy(table2[join_key])
out1 = df.count().withColumnRenamed("count", "Cnt")
out2 = df.agg(sf.max(table1[agg_key]).alias("Max1"), sf.max(table2[agg_key]).alias("Max2"))

script_output[0] = out1
script_output[1] = out2

print("----- BEGIN SCRIPT OUTPUT -----")
print("This is my output!")

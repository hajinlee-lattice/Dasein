for idx, tgt in enumerate(script_targets):
    if idx not in script_output:
        raise ValueError("Cannot find %d-th output dataframe" % idx)
    df = script_output[idx]
    df.write.format("com.databricks.spark.avro").save(tgt['Path'])
    count = df.count()
    tgt['StorageType'] = 'Hdfs'
    tgt['Count'] = count

print("----- BEGIN SCRIPT OUTPUT -----")
print(json.dumps(script_targets))


if len(lattice.targets) != len(lattice.output):
    raise ValueError(
        "{} targets are declared but {} outputs are generated!".format(len(lattice.targets), len(lattice.output)))

for tgt, df in zip(lattice.targets, lattice.output):
    df = df
    fmt = tgt['DataFormat'].lower() if 'DataFormat' in tgt else "avro"
    partition_keys = tgt['PartitionKeys'] if 'PartitionKeys' in tgt else []
    if not partition_keys:
        df.write.format(fmt).save(tgt['Path'])
    else:
        df.write.partitionBy(*partition_keys).format(fmt).save(tgt['Path'])
    df2 = spark.read.format(fmt).load(tgt['Path'])
    tgt['StorageType'] = 'Hdfs'
    tgt['Count'] = df2.count()

print("----- BEGIN SCRIPT OUTPUT -----")
print(json.dumps(lattice.targets))

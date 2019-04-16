if len(lattice.targets) != len(lattice.output):
    raise ValueError(
        "{} targets are declared but {} outputs are generated!".format(len(lattice.targets), len(lattice.output)))

for tgt, df in zip(lattice.targets, lattice.output):
    df = df
    df.write.format("avro").save(tgt['Path'])
    df2 = spark.read.format("avro").load(tgt['Path'])
    count = df2.count()
    df.unpersist()
    tgt['StorageType'] = 'Hdfs'
    tgt['Count'] = count

print("----- BEGIN SCRIPT OUTPUT -----")
print(json.dumps(lattice.targets))

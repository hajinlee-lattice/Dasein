from pyspark.storagelevel import StorageLevel

if len(lattice.targets) != len(lattice.output):
    raise ValueError(
        "{} targets are declared but {} outputs are generated!".format(len(lattice.targets), len(lattice.output)))

for tgt, df in zip(lattice.targets, lattice.output):
    df = df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    df.write.format("avro").save(tgt['Path'])
    count = df.count()
    df.unpersist()
    tgt['StorageType'] = 'Hdfs'
    tgt['Count'] = count

print("----- BEGIN SCRIPT OUTPUT -----")
print(json.dumps(lattice.targets))

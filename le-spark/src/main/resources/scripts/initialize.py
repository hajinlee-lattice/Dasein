import json

class LatticeContext:
    def __init__(self, input, params, targets):
        self.input = input
        self.params = params
        self.targets = targets
        self.output = []
        self.output_str = ""

def load_data_unit(unit):
    storage = unit['StorageType'].lower()
    if storage == "hdfs":
        return load_hdfs_unit(unit)
    else:
        raise ValueError("Unsupported storage type %s" % storage)

def load_hdfs_unit(unit):
    path = unit['Path']
    if path[-5:] != ".avro":
        if path[-1] == "/":
            path += "*.avro"
        else:
            path += "/*.avro"
    path = "hdfs://%s" % path
    return spark.read.format("avro").load(path)

checkpoint_dir = '''{{CHECKPOINT_DIR}}'''
if checkpoint_dir != "":
    print("----- BEGIN SCRIPT OUTPUT -----")
    print("Checkpoint Dir:", checkpoint_dir)
    print("----- END SCRIPT OUTPUT -----")
    spark.sparkContext.setCheckpointDir(checkpoint_dir)

script_targets = json.loads('''{{TARGETS}}''')
print("----- BEGIN SCRIPT OUTPUT -----")
print("Targets:", script_targets)
print("----- END SCRIPT OUTPUT -----")

raw_input = json.loads('''{{INPUT}}''')
script_input = [load_data_unit(unit) for unit in raw_input]

print("----- BEGIN SCRIPT OUTPUT -----")
print("Input:", script_input)
print("----- END SCRIPT OUTPUT -----")

script_params = json.loads('''{{PARAMS}}''')

print("----- BEGIN SCRIPT OUTPUT -----")
print("Params: %s" % json.dumps(script_params))

lattice = LatticeContext(input=script_input, params=script_params, targets=script_targets)

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


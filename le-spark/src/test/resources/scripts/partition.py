if lattice.params["Partition"]:
    set_partition_targets(0, ["Field1", "Field2", "Field3", "Field4", "Field5"], lattice)

result = lattice.input[0]

# finish
lattice.output = [result]
lattice.output_str = "This is script output!"

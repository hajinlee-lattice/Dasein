
if lattice.params["Partition"]==True:
    set_partition_targets(0, ["Field2"], lattice)
else:
    set_partition_targets(0, None, lattice)

result = lattice.input[0]

lattice.output = [result]
lattice.output_str = "This is my output!"
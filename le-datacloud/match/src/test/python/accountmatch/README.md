Account Entity Match Test Data Generator
============================

[Design doc for test groups](https://confluence.lattice-engines.com/x/zA7BAw)

# Prerequisites

1. Python 2.7
2. `avro` module installed

**TODO** Not sure if this works with python3
**TODO** Not very familiar with python yet, probably have `requirements.txt` or something similar later.

# How to Add New Test

Each test contains one or multiple test groups. Follow the steps below to create a new test.

1. Create a python file in `/accountmatchtests` directory. E.g., `example.py`
2. Construct test groups using classes in `/testgroup` directory.
3. Create a method called `get_test_groups` that returns a list of constructed test groups.

# How to Generate Test Data from Existing Test

Entry point is `accountmatch.py` script.

1. Use `-t` to specify which test file in `/accountmatchtests` you want. E.g., `-t example` to use `/accountmatchtests/example.py`. Note that no file extension is included.
2. Use `-f` to specify output format (either `avro` or `csv`).
3. Specify output file name as the first argument after the script. Again no need to specify extension.
4. Two files will be generated in `/output` directory. One for data that exists before testing, the other for test data.

**Example**

- Use `/accountmatchtests/example.py` to generate test data in CSV format.
```
python accountmatch.py -t example -f csv output
```
- Console output will looks like the snippet below. You can see how many rows are generated and the path of the files.
```
==================================
Output Files:
Existing Data (43 rows): /Users/slin/Documents/Programming/accountmatchtestdata/output/output_existing.csv
Test Data (560 rows): /Users/slin/Documents/Programming/accountmatchtestdata/output/output_test.csv
==================================
```

# Notes

1. Use `-h` to see the script usage. E.g., `python accountmatch.py -h`
2. We use a fixed seed for random to generate deterministic test data (run script multiple times and you get the same test file). Use `-s <seed>` to change the seed.
3. Use `-v` to get verbose console output.
4. Run `bash clean_output.sh` to cleanup `/output` directory
5. Currently this script overrides test file in `/output` if some file with the same name already exists.

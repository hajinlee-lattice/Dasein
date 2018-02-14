from __future__ import print_function

import avro.schema
import random
import sys
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


def reduceFileSize(fileName, percentageOfLines):
    fileReader = DataFileReader(open(fileName, "r"), DatumReader())
    fileWriter = DataFileWriter(open(fileName.replace(".avro", "reduced.avro"), "w") , DatumWriter(),
                                avro.schema.parse(fileReader.meta["avro.schema"]))

    for user in fileReader:
        if random.random() < percentageOfLines:
            fileWriter.append(user)

    fileReader.close()
    fileWriter.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python reducesize.py <avrofile> <percentreduction>")
        print(" where 0.1 is percentage of rows to take")
    fileName = sys.argv[1]
    numberOfLines = float(sys.argv[2])
    reduceFileSize(fileName, numberOfLines)




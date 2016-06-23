import base64
import csv
import glob
import gzip
import json
import os
from random import shuffle
import shutil
import sys
import uuid
import zipfile

from leframework import scoringengine as se
from leframework.argumentparser import ArgumentParser
import numpy as np
from pipelinefwk import ModelStep
from testbase import TestBase
from testbase import removeFiles


class TrainingTestBase(TestBase):

    def setUp(self):
        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        os.environ["CONTAINER_ID"] = "container_1425511391553_3644_01_000001"
        os.environ["SHDP_HD_FSWEB"] = "http://localhost:50070"
        os.environ["DEBUG"] = "true"
        # Simulate what happens in yarn when it copies the framework code over
        # before running the python script
        self.fwkdir = "./leframework.tar.gz"
        self.pipelinefwkdir = "./lepipeline.tar.gz"
        self.evpipelinefwkdir = "./evpipeline.tar.gz"
        self.swlibdir = "./le-serviceflows-leadprioritization-2.0.22-SNAPSHOT.jar"
        fwkdir = self.fwkdir
        pipelinefwkdir = self.pipelinefwkdir
        evpipelinefwkdir = self.evpipelinefwkdir
        swlibdir = self.swlibdir

        if os.path.exists(fwkdir):
            shutil.rmtree(fwkdir)
        if os.path.exists(pipelinefwkdir):
            shutil.rmtree(pipelinefwkdir)
        if os.path.exists(evpipelinefwkdir):
            shutil.rmtree(evpipelinefwkdir)
        if os.path.exists(swlibdir):
            shutil.rmtree(swlibdir)

        os.makedirs(fwkdir + "/leframework")
        os.makedirs(pipelinefwkdir)
        os.makedirs(evpipelinefwkdir)
        os.makedirs(swlibdir)

        enginedir = "/leframework/scoringengine.py"

        basePath = "../../" if os.path.exists("../../main/python/rulefwk.py") else "../../../"

        os.symlink(basePath + "/main/python/pipelinefwk.py", "./pipelinefwk.py")
        os.symlink(basePath + "/main/python/pipeline/pipeline.py", "pipeline.py")
        os.symlink(basePath + "/main/python/configurablepipelinetransformsfromfile/pipeline.json", "pipeline.json")
        os.symlink(basePath + "/main/python/configurablepipelinetransformsfromfile/pmmlpipeline.json", "pmmlpipeline.json")
        os.symlink(basePath + "/main/python/configurablepipelinetransformsfromfile/pipelinenullconversionrate.json", "pipelinenullconversionrate.json")
        os.symlink(basePath + "/main/python/evpipeline/evpipeline.py", "evpipeline.py")

        shutil.copy(basePath + "/main/python" + enginedir, fwkdir + enginedir)

        for filename in glob.glob(os.path.join(basePath + "/main/python/pipeline", "*.py")):
            shutil.copy(filename, pipelinefwkdir)

        for filename in glob.glob(os.path.join(basePath + "/main/python/evpipeline", "*.py")):
            shutil.copy(filename, evpipelinefwkdir)
        shutil.copy(basePath + "/main/python/pipeline/encoder.py", evpipelinefwkdir)

        for filename in glob.glob(os.path.join(basePath + "/main/python/configurablepipelinetransformsfromfile", "*")):
            if filename.find("/pipelinenullconversionrate.json") >= 0:
                continue
            shutil.copy(filename, pipelinefwkdir)
            shutil.copy(filename, evpipelinefwkdir)

        sys.path.append(pipelinefwkdir)
        sys.path.append(evpipelinefwkdir)

        # Symbolic links will be cleaned up by testBase
        scriptDir = basePath + "/main/python/algorithm/"
        for f in os.listdir(scriptDir):
            fPath = os.path.join(scriptDir, f)
            if os.path.isfile(fPath) and not os.path.exists(f):
                os.symlink(fPath, f)

        results = "./results"
        if os.path.exists(results):
            shutil.rmtree(results)

        self.__unzipSoftwareLibJar()

    def __unzipSoftwareLibJar(self):
        zipFilePath = "data/le-serviceflows-leadprioritization-2.0.22-SNAPSHOT.zip" if \
            os.path.exists("data/le-serviceflows-leadprioritization-2.0.22-SNAPSHOT.zip") \
            else "../data/le-serviceflows-leadprioritization-2.0.22-SNAPSHOT.zip"
        with zipfile.ZipFile(zipFilePath) as z:
            z.extractall(self.swlibdir)

    def tearDown(self):
        if os.path.exists(self.fwkdir):
            shutil.rmtree(self.fwkdir)
        if os.path.exists(self.pipelinefwkdir):
            shutil.rmtree(self.pipelinefwkdir)
        if os.path.exists(self.evpipelinefwkdir):
            shutil.rmtree(self.evpipelinefwkdir)
        if os.path.exists(self.swlibdir):
            shutil.rmtree(self.swlibdir)
        removeFiles(".")
        removeFiles("./results")

    def getLineToScore2(self, inputColumns, typeDict, value):
        columnWithValue = zip(inputColumns, value)
        line = self.getLine(columnWithValue, typeDict)
        return line

    def getLineToScore(self, inputColumns, typeDict, value):
        columnWithValue = zip(inputColumns, value)
        line1 = self.getLine(columnWithValue, typeDict)

        shuffle(columnWithValue)
        line2 = self.getLine(columnWithValue, typeDict)

        return (line1, line2)

    def getLine(self, columnsWithValue, typeDict):
        line = "["
        first = True
        for i in range(len(columnsWithValue)):
            if first:
                first = False
            else:
                line += ","
            if typeDict[columnsWithValue[i][0]] == '-9':
                line += "{\"Key\":\"%s\",\"Value\":{\"SerializedValueAndType\":\"String|'%s'\"}}" % (columnsWithValue[i][0], columnsWithValue[i][1])
            else:
                line += "{\"Key\":\"%s\",\"Value\":{\"SerializedValueAndType\":\"Float|'%s'\"}}" % (columnsWithValue[i][0], columnsWithValue[i][1])
        line += "]"
        line = '{"key":"%s","value":%s}' % (str(uuid.uuid4()), line)
        return line

    def createCSV(self, inputColumns, values):
        with open('./results/test.csv', 'wb') as csvfile:
            csvWriter = csv.writer(csvfile)
            csvWriter.writerow(['id'] + inputColumns)
            for i in range(len(values)):
                csvWriter.writerow([i + 1] + values[i])

    def stripPath(self, fileName):
        return fileName[fileName.rfind('/') + 1:len(fileName)]


    def createCSVFromModel(self, modelFile, scoringFile, training=False):
        parser = ArgumentParser(modelFile, None)
        schema = parser.getSchema()

        if training:
            data = parser.createList(self.stripPath(schema["training_data"]))
        else:
            data = parser.createList(self.stripPath(schema["test_data"]))
        data.reset_index()
        fields = { k['name']:k['type'][0] for k in parser.fields }

        with open(scoringFile, "w") as fp:
            i = 1
            for row in data.iterrows():
                line = "["
                first = True
                for field in fields.keys():
                    value = None
                    if field in row[1]:
                        value = row[1][field]
                    else:
                        continue
                    if first:
                        first = False
                    else:
                        line += ","
                    dataType = 'String' if fields[field] == 'string' or fields[field] == 'bytes' else 'Float'
                    # print ("Row %d Column = %s, Type = %s" % (i, field, type(value)))

                    if row[1][field] is None or (dataType == 'Float' and np.isnan(float(row[1][field]))):
                        line += "{\"Key\":\"%s\",\"Value\":{\"SerializedValueAndType\":\"%s|\"}}" % (field, dataType)
                    elif dataType == 'String':
                        line += "{\"Key\":\"%s\",\"Value\":{\"SerializedValueAndType\":\"String|'%s'\"}}" % (field, json.dumps(value)[1:-1])
                    else:
                        line += "{\"Key\":\"%s\",\"Value\":{\"SerializedValueAndType\":\"Float|'%s'\"}}" % (field, str(float(value)))
                line += "]"
                line = '{"key":"%s","value":%s}' % (str(uuid.uuid4()), line)
                fp.write(line + "\n")
                i += 1

    def getPredictScore(self, pipeline, inputColumns, typeDict, values):
        scores = []
        for value in values:
            row = self.getLine(zip(inputColumns, value), typeDict)
            rowDicts = []
            rowDicts.append(se.getRowToScore(row)[1])
            resultFrame = se.predict(pipeline, rowDicts)
            scores.append(resultFrame['Score'][0])
        return scores

    def decodeBase64ThenDecompressToFile(self, data, filename):
        gzipByteArray = bytearray(base64.decodestring(data))
        with open(filename, "wb") as output:
            output.write(gzipByteArray)

        with gzip.GzipFile(filename, "rb") as compressed:
            data = compressed.read()
            with open(filename, "wb") as decompressed:
                decompressed.write(data)

        return decompressed.name

    def getModelStep(self, pipeline):
        return [s for s in pipeline.getPipeline() if isinstance(s, ModelStep)][0]

    def ignore(self, ignore):
        def _ignore_(path, names):
            ignoredNames = []
            if ignore in names:
                ignoredNames.append(ignore)
            return set(ignoredNames)
        return _ignore_

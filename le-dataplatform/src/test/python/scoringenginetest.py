import os
import subprocess
import sys
import base64
import json
import glob
import gzip
from trainingtestbase import TrainingTestBase

class ScoringEngineTest(TrainingTestBase):
    inputFileName = "./scoringtestinput.txt"
    outputFileName = "./results/scoringtestoutput.txt"
 
    def testScoringEngine(self):
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        traininglauncher = Launcher("model.json")
        traininglauncher.execute(False)
        traininglauncher.training

        # Retrieve the pickled model from the json file
        jsonDict = json.loads(open(glob.glob("./results/*.json")[0]).read())

        for index in range(0, len(jsonDict["Model"]["CompressedSupportFiles"])):
            entry = jsonDict["Model"]["CompressedSupportFiles"][index]
            fileName = "./results/" + entry["Key"] + ".gz"
            self.decodeBase64ThenDecompressToFile(entry["Value"], fileName)
            os.rename(fileName, "./results/" + entry["Key"])

        scoringScript = open("./results/scoringengine.py", "w")
        scoringScript.write(jsonDict["Model"]["Script"])
        scoringScript.close()

        os.environ["PYTHONPATH"] = ''
        subprocess.call([sys.executable, './results/scoringengine.py', self.inputFileName, self.outputFileName])
        with open(self.inputFileName) as f:
            self.inputKeys = [json.loads(line)["key"] for line in f]
        f.close

        self.assertEqual(len(self.inputKeys), 5000)
        with open(self.outputFileName) as f:
            for i in range(0, len(self.inputKeys)):
                line = f.readline()
                result = line.split(',')
                self.assertEqual(result[0], self.inputKeys[i])
                self.assertEqual(len(result), 2)
                self.assertGreaterEqual(float(result[1].rstrip()), 0)
                self.assertLessEqual(float(result[1].rstrip()), 1)
        f.close()

    def decodeBase64ThenDecompressToScript(self, data, filename):
        gzipByteArray = bytearray(base64.decodestring(data))
        with open(filename, "wb") as output:
            output.write(gzipByteArray)
        output.close()

        with gzip.GzipFile(filename, "rb") as compressed:
            data = compressed.read()
            with open(filename.rstrip('.gz'), "wb") as decompressed:
                decompressed.write(data)
        compressed.close()
        decompressed.close()
        return decompressed.name

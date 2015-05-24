import csv
import glob
import json
import os
import subprocess
import sys
from trainingtestbase import TrainingTestBase

class ScoringEngineTest(TrainingTestBase):
 
    def testScoringEngine(self):
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        traininglauncher = Launcher("modeldriver-mulesoft-scoring.json")
        traininglauncher.execute(False)
        traininglauncher.training
        
        # Retrieve the pickled model from the json file
        jsonDict = json.loads(open(glob.glob("./results/*.json")[0]).read())

        for index in range(0, len(jsonDict["Model"]["CompressedSupportFiles"])):
            entry = jsonDict["Model"]["CompressedSupportFiles"][index]
            fileName = "./results/" + entry["Key"] + ".gz"
            self.decodeBase64ThenDecompressToFile(entry["Value"], fileName)
            os.rename(fileName, "./results/" + entry["Key"])

        with open("./results/scoringengine.py", "w") as scoringScript:
            scoringScript.write(jsonDict["Model"]["Script"])

        self.createCSVFromModel("modeldriver-mulesoft-scoring.json", "./results/scoreinputfile.txt")
        os.environ["PYTHONPATH"] = ''
        popen = subprocess.Popen([sys.executable, "./results/scoringengine.py", "./results/scoreinputfile.txt", "./results/scoreoutputfile.txt"], \
                         stdout = subprocess.PIPE, stderr=subprocess.PIPE)
        _, stderr = popen.communicate()
        self.assertEquals(len(stderr), 0)

        tokens = csv.reader(open("./results/scoreoutputfile.txt", "r")).next()
        self.assertEquals(len(tokens), 2)
        
        scored = []
        with open(glob.glob("./results/*_scored.txt")[0]) as fs:
            reader = csv.reader(fs)
            for row in reader:
                scored.append(row[1])
        output = []
        with open("./results/scoreoutputfile.txt", "r") as fs:
            reader = csv.reader(fs)
            for row in reader:
                output.append(row[1])
        
        for i in xrange(len(output)):
            self.assertEquals(scored[i], output[i])


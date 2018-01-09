import csv
import glob
import json
import os
import subprocess
import sys
import shutil
from unittest import TestCase

modelID = "2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.json"
SCORING_SCRIPT_NAME = "scoringengine.py"
PIPELINE_SCRIPT_NAME = "pipeline.py"
PICKEL_FILE_NAME = "STPipelineBinary.p"
PIPELINEFWK_SCRIPT_NAME = "pipelinefwk.py"
ENCODER_SCRIPT_NAME = "encoder.py"
PIPELINESTEPS_SCRIPT_NAME = "pipelinesteps.py"

def deleteUnwatedFiles():
    currentFiles = os.listdir(os.getcwd())
    for currentFile in currentFiles:
        if currentFile != "scoringtest.py":
            os.remove(currentFile)
    print os.listdir(os.getcwd())

def getSupportedFiles(srcFile):
    if "output" in srcFile:
        dst = os.getcwd() + "/" + srcFile[srcFile.rfind('/')+1:]
    elif "input" in srcFile:
        dst = os.getcwd() + "/" + modelID +"-0"
    else:
        dst = os.getcwd() + "/" + modelID + srcFile[srcFile.rfind('/')+1:]
    print dst
    shutil.copy(srcFile, dst)
    
def filesAreDeleted(scoringSupportedFiles):
    currentFiles = os.listdir(os.getcwd())
    print currentFiles
    for currentFile in currentFiles:
        print currentFile
        for scoringSupportedFile in scoringSupportedFiles:
            if currentFile == scoringSupportedFile:
                return False;
                break;
    return True;

class ScoringEngineTest(TestCase):
 
    def testScoringEngine(self):
        
        # copy over and rename supported files except for the scoringoutputfile.txt
        supportedFiles = glob.glob("../../resources/com/latticeengines/scoring/models/supportedFiles/*")
        for file in supportedFiles:
            getSupportedFiles(file)
        
        # copy over scoring.py
        shutil.copy("../../../main/python/scoring.py", os.getcwd())
 
        os.environ["PYTHONPATH"] = ''
        executable = "/usr/local/bin/python2.7"
        popen = subprocess.Popen([executable, "./scoring.py", "Json", modelID], \
                         stdout = subprocess.PIPE, stderr=subprocess.PIPE)
        s, stderr = popen.communicate()
        print s
        print stderr
        #self.assertEquals(len(stderr), 0)

        # make sure that all supported files have been deleted
        scoringSupportedFiles = [SCORING_SCRIPT_NAME, PIPELINE_SCRIPT_NAME, PICKEL_FILE_NAME, PIPELINEFWK_SCRIPT_NAME, ENCODER_SCRIPT_NAME, PIPELINESTEPS_SCRIPT_NAME]
        self.assertTrue(filesAreDeleted(scoringSupportedFiles), "The scoring supported files be deleted.")
        
        # make sure the scores are correct
        tokens = csv.reader(open("2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.jsonscoringoutputfile-0.txt", "r")).next()
        self.assertEquals(len(tokens), 2)
          
        scored = []
        with open("2Checkout_relaunch_PLSModel_2015-03-19_15-37_model.jsonscoringoutputfile-0.txt", "r") as fs:
            reader = csv.reader(fs)
            for row in reader:
                scored.append(row[1])
        output = []
        with open("scoreoutputfile.txt", "r") as fs:
            reader = csv.reader(fs)
            for row in reader:
                output.append(row[1])
          
        for i in xrange(len(output)):
            self.assertTrue(abs(float(scored[i]) - float(output[i])) < 0.00001)

        # delete unwanted files
        deleteUnwatedFiles()
        self.assertTrue(len(os.listdir(os.getcwd()))==1, "All the files except for scoringtest.py should be deleted")

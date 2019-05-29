import os

import shutil
import subprocess
import sys

SCORING_SCRIPT_NAME = "scoringengine.py"
PIPELINE_SCRIPT_NAME = "pipeline.py"
PICKLE_FILE_NAME = "STPipelineBinary.p"
PIPELINEFWK_SCRIPT_NAME = "pipelinefwk.py"
ENCODER_SCRIPT_NAME = "encoder.py"
PIPELINESTEPS_SCRIPT_NAME = "pipelinesteps.py"
SCORING_INPUT_PREFIX = "scoringinputfile-"
SCORING_OUTPUT_PREFIX = "scoringoutputfile-"

def main(argv):
    type = argv[1]
    print 'type is :' + type
    if (type == 'Avro'): 
        leadFiles = filter(lambda f: f.endswith('.avro'), os.listdir('.'))
        for index in range(3, len(argv)):
            scoringSupportedFiles = []
            # change the file name of supported files
            modelID = argv[index]
            manipulateSupportedFiles(modelID, scoringSupportedFiles)
            print "leadFiles are: ", leadFiles
            print "scoringSupportedFiles are: " , scoringSupportedFiles
            if len(leadFiles) == 0:
                raise "leadFile is null"
            for leadFile in leadFiles:
                if not leadFile.start(modelID):
                    continue
                modelEvaluateAvro(modelID, leadFile, type, argv[2])
            # delete the supported files for the next round of scoring
            deleteFiles(scoringSupportedFiles)
            deletePycFiles()
    else :
        for index in range(2, len(argv)):
            leadFiles = []
            scoringSupportedFiles = []
            # change the file name of supported files
            modelID = argv[index]
            manipulateInputFiles(modelID, scoringSupportedFiles, leadFiles)
            manipulateSupportedFiles(modelID, scoringSupportedFiles)
            # do scoring
            print "leadFiles are: ", leadFiles
            print "scoringSupportedFiles are: " , scoringSupportedFiles
            if len(leadFiles) == 0:
                raise "leadFile is null"
            for leadFile in leadFiles:
                modelEvaluateJson(modelID, leadFile, type)
            # delete the supported files for the next round of scoring
            deleteFiles(scoringSupportedFiles)
            deleteFiles(leadFiles)
            deletePycFiles()

def manipulateSupportedFiles(modelID, scoringSupportedFiles):
    # loop through all the files in the current directory
    files = os.listdir('.')
    for f in files:
        if f.startswith(modelID) and (f.endswith('.json') or f.endswith('.py') or f.endswith('.p')):
            try:
                os.rename(f, f[len(modelID):])
                print "renaming ", f, "to:", f[len(modelID):]
                scoringSupportedFiles.append(f[len(modelID):])
            except:
                print "scoring.py: error when trying to rename ", f
                pass

def manipulateInputFiles(modelID, scoringSupportedFiles, leadFiles):
    s = modelID + "-"
    # loop through all the files in the current directory
    files = os.listdir('.')
    for f in files:
        if f.startswith(s):
            updatedLeadName = SCORING_INPUT_PREFIX + f[len(s):]
            os.rename(f, updatedLeadName)
            leadFiles.append(updatedLeadName)
            continue

def modelEvaluateJson(modelID, leadFile, type):
    scoringScriptPath = os.path.abspath(SCORING_SCRIPT_NAME)
    # scoringoutputfile name
    outputFile = modelID + SCORING_OUTPUT_PREFIX + leadFile[len(SCORING_INPUT_PREFIX):] + ".txt"
    popen = subprocess.Popen([sys.executable, scoringScriptPath, leadFile, outputFile], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s, stderr = popen.communicate()
    print s
    print stderr

def modelEvaluateAvro(modelID, leadFile, type, idColName):
    scoringScriptPath = os.path.abspath(SCORING_SCRIPT_NAME)
    # scoringoutputfile name
    outputFile = modelID + SCORING_OUTPUT_PREFIX + "0.txt"
    popen = subprocess.Popen([sys.executable, scoringScriptPath, leadFile, outputFile, idColName], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s, stderr = popen.communicate()
    print s
    print stderr

def deleteFiles(files):
    for f in files:
        try:
            os.remove(f)
        except OSError, e:
            print ("Error: %s - %s." % (e.filename, e.strerror))

def deletePycFiles():
    dir = os.listdir(os.getcwd())
    for f in dir:
        ext = '.pyc'
        if f.lower().endswith(ext):
            print 'found file:', f
            os.remove(f)

if __name__ == "__main__":
    sys.exit(main(sys.argv))

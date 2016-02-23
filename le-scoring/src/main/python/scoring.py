import sys
import os
import subprocess
import shutil

SCORING_SCRIPT_NAME = "scoringengine.py"
PIPELINE_SCRIPT_NAME = "pipeline.py"
PICKEL_FILE_NAME = "STPipelineBinary.p"
PIPELINEFWK_SCRIPT_NAME = "pipelinefwk.py"
ENCODER_SCRIPT_NAME = "encoder.py"
PIPELINESTEPS_SCRIPT_NAME = "pipelinesteps.py"
SCORING_INPUT_PREFIX = "scoringinputfile-"
SCORING_OUTPUT_PREFIX = "scoringoutputfile-"

IMPUTATION_STEP_SCRIPT_NAME="imputationstep.py"
IMPUTATION_STEP_EVPIPELINE_SCRIPT_NAME="imputationstepevpipeline.py"
MAKE_FLOAT_SCRIPT_NAME="make_float.py"
REPLACE_NULL_VALUE_SCRIPT_NAME="replace_null_value.py"
COLUMNTRANFORM_SCRIPT_NAME="columntransform.py"
REVENUE_COLUMN_SCRIPT_NAME="revenuecolumntransformstep.py"
CLEANCATEGORICAL_SCRIPT_NAME="cleancategoricalcolumn.py"
AGGREGATED_MODEL_SCRIPT_NAME="aggregatedmodel.py"
ENUMERATED_COLUMN_SCRIPT_NAME="enumeratedcolumntransformstep.py"
COLUMNTYPE_CONVERSION_SCRIPT_NAME="columntypeconversionstep.py"

def main(argv):
    scoringFiles = [SCORING_SCRIPT_NAME, PIPELINE_SCRIPT_NAME, PICKEL_FILE_NAME, PIPELINEFWK_SCRIPT_NAME, ENCODER_SCRIPT_NAME, PIPELINESTEPS_SCRIPT_NAME,
                    IMPUTATION_STEP_SCRIPT_NAME,
                    IMPUTATION_STEP_EVPIPELINE_SCRIPT_NAME,
                    MAKE_FLOAT_SCRIPT_NAME,
                    REPLACE_NULL_VALUE_SCRIPT_NAME,
                    COLUMNTRANFORM_SCRIPT_NAME,
                    REVENUE_COLUMN_SCRIPT_NAME,
                    CLEANCATEGORICAL_SCRIPT_NAME,
                    AGGREGATED_MODEL_SCRIPT_NAME,
                    ENUMERATED_COLUMN_SCRIPT_NAME,
                    COLUMNTYPE_CONVERSION_SCRIPT_NAME]
    print scoringFiles
    for index in range(len(argv)):
        leadFiles = []
        if index == 0:
            continue
        #change the file name of supported files
        modelID = argv[index]
        manipulateSupportedFiles(modelID, scoringFiles, leadFiles)    
        #do scoring
        print "leadFiles are:"
        print leadFiles
        if len(leadFiles) == 0:
            raise "leadFile is null"
        for leadFile in leadFiles:
            modelEvaluate(modelID, leadFile)
        #delete the supported files for the next round of scoring
        deleteFiles(scoringFiles)
        deleteFiles(leadFiles)
        deletePycFiles()

def manipulateSupportedFiles(modelID, scoringFiles, leadFiles):
    s = modelID + "-"
    curFiles = os.listdir(os.getcwd())
    targetedFiles = []
    for f in scoringFiles:
        targetedFiles.append(modelID+f)
    # loop through all the files in the current directory
    files = os.listdir('.')
    for f in files:
        if f.startswith(s):
            updatedLeadName = SCORING_INPUT_PREFIX+f[len(s):]
            os.rename(f, updatedLeadName)
            leadFiles.append(updatedLeadName)
            continue
        for fileName in targetedFiles:
            if f.startswith(fileName):
                os.rename(f, fileName[len(modelID):])
    print "after the renaming, the files are"
    print os.listdir('.')   
 
def modelEvaluate(modelID, leadFile):
    scoringScriptPath = os.path.abspath(SCORING_SCRIPT_NAME)
    #scoringoutputfile name
    outputFile = modelID + SCORING_OUTPUT_PREFIX + leadFile[len(SCORING_INPUT_PREFIX):] + ".txt"
    executable = "/usr/local/bin/python2.7"
    popen = subprocess.Popen([executable, scoringScriptPath, leadFile, outputFile], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
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
        ext ='.pyc'
        if f.lower().endswith(ext):
            print 'found file: '
            print f
            os.remove(f)

if __name__ == "__main__":
    sys.exit(main(sys.argv))

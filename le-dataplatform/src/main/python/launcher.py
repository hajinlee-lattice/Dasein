import logging
import numpy
import os
import pwd
import sys
from leframework import argumentparser as ap
from leframework import webhdfs
from sklearn.externals import joblib
from urlparse import urlparse

logging.basicConfig(level = logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='launcher')

def stripPath(fileName):
    return fileName[fileName.rfind('/')+1:len(fileName)]

def validateEnvVariable(variable):
    try:
        os.environ[variable]
    except KeyError:
        raise Exception("%s environment variable not set." % (variable))

def validateSchemaParam(schema, param):
    try:
        schema[param]
    except KeyError:
        raise Exception("%s not set in job metadata." % (param))
    
def validateEnvAndParameters(schema):
    validateEnvVariable('SHDP_HD_FSWEB')
    validateEnvVariable('CONTAINER_ID')
    validateSchemaParam(schema, "training_data")
    validateSchemaParam(schema, "test_data")
    validateSchemaParam(schema, "python_script")
    validateSchemaParam(schema, "model_data_dir")
    
def writeScoredTestData(testingData, schema, clf, modelDir):
    scored = clf.predict_proba(testingData[:, schema["featureIndex"]])
    scored = numpy.apply_along_axis(lambda x: [x[1]], 1, scored)
    keyData = testingData[:, schema["keyColIndex"]]
    scored = numpy.concatenate((keyData, scored), axis=1)
    numpy.savetxt(modelDir + "scored.txt", scored, delimiter=",")
    
    
def getModelDirPath(schema):
    appIdList = os.environ['CONTAINER_ID'].split("_")[1:3]
    modelDirPath = "%s/%s" % (schema["model_data_dir"], "_".join(appIdList))
    return modelDirPath

def populateSchemaWithMetadata(schema, parser):
    schema["featureIndex"] = parser.getFeatureTuple()
    schema["targetIndex"] = parser.getTargetIndex()
    schema["keyColIndex"] = parser.getKeyColumns()

if __name__ == "__main__":
    """
    Transform the inputs into python objects and invoke user python script.
    
    Arguments:
    sys.argv[1] -- schema json file
    """

    parser = ap.ArgumentParser(sys.argv[1])
    schema = parser.getSchema()
    
    # Fail fast if required parameters are not set
    validateEnvAndParameters(schema)
    
    training = parser.createList(stripPath(schema["training_data"]))
    test = parser.createList(stripPath(schema["test_data"]))
    script = stripPath(schema["python_script"])
    populateSchemaWithMetadata(schema, parser)
    execfile(script)

    # Create directory for model result
    modelFileDir = os.getcwd() + "/results/"
    os.mkdir(modelFileDir)
    
    # Get algorithm properties
    algorithmProperties = parser.getAlgorithmProperties()
    
    # Execute the packaged script from the client and get the returned file
    # that contains the generated model data
    clf = globals()['train'](training, test, schema, modelFileDir, algorithmProperties)
    # Pickle the generated model and store in model.pkl file
    # and create the score file from the hold out set
    if clf != None:
        joblib.dump(clf, modelFileDir + '/model.pkl', compress = 9)
        writeScoredTestData(test, schema, clf, modelFileDir)
    else:
        logger.error("Generated classifier is null!")
    
    # Create webhdfs instance for writing to hdfs
    webHdfsHostPort = urlparse(os.environ['SHDP_HD_FSWEB'])
    hdfs = webhdfs.WebHDFS(webHdfsHostPort.hostname, webHdfsHostPort.port, pwd.getpwuid(os.getuid())[0])
    
    # Create model directory
    modelDirPath = getModelDirPath(schema)
    hdfs.mkdir(modelDirPath)
    
    # Copy the model data files from local to hdfs
    filenames = []
    (_, _, filenames) = os.walk(modelFileDir).next()
    for filename in filenames:
        hdfs.copyFromLocal(modelFileDir + filename, "%s/%s" % (modelDirPath, filename))
     
    

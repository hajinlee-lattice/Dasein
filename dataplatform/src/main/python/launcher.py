import os
import pwd
import sys
from leframework import argumentparser
from leframework import webhdfs
from urlparse import urlparse


def stripPath(fileName):
    #return fileName
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
    
def getModelDirPath(schema):
    appIdList = os.environ['CONTAINER_ID'].split("_")[1:3]
    modelDirPath = "%s/%s" % (schema["model_data_dir"], "_".join(appIdList))
    return modelDirPath

if __name__ == "__main__":
    """
    Transform the inputs into python objects and invoke user python script.
    
    Arguments:
    sys.argv[1] -- schema json file
    """

    parser = argumentparser.createParser(stripPath(sys.argv[1]))
    schema = parser.getSchema()
    
    # Fail fast if required parameters are not set
    validateEnvAndParameters(schema)
    
    training = parser.createList(stripPath(schema["training_data"]))
    test = parser.createList(stripPath(schema["test_data"]))
    script = stripPath(schema["python_script"])
    execfile(script)
    
    # Execute the packaged script from the client and get the returned file
    # that contains the generated model data
    modelFilePath = globals()['train'](training, test, schema)
    
    webHdfsHostPort = urlparse(os.environ['SHDP_HD_FSWEB'])
    hdfs = webhdfs.createWebHdfs(webHdfsHostPort.hostname, webHdfsHostPort.port, pwd.getpwuid(os.getuid())[0])
    
    # Create model directory
    modelDirPath = getModelDirPath(schema)
    hdfs.mkdir(modelDirPath)
    
    # Copy the model data file from local to hdfs
    hdfsFilePath = stripPath(modelFilePath)
    hdfs.copyFromLocal(modelFilePath, "%s/%s" % (modelDirPath, hdfsFilePath))
     
    

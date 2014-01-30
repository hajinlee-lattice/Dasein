import os
import pwd
import sys
from leframework import argumentparser
from leframework import webhdfs
from urlparse import urlparse


def stripPath(fileName):
    #return fileName
    return fileName[fileName.rfind('/')+1 : fileName.__len__()]

if __name__ == "__main__":
    """
    Transform the inputs into python objects and invoke user python script.
    
    Arguments:
    sys.argv[1] -- training data file
    sys.argv[2] -- test data file
    sys.argv[3] -- schema json file
    sys.argv[4] -- user python script
    """

    parser = argumentparser.createParser(stripPath(sys.argv[3]))
    training = parser.createList(stripPath(sys.argv[1]))
    test = parser.createList(stripPath(sys.argv[2]))
    script = stripPath(sys.argv[4])
    execfile(script)
    schema = parser.getSchema()
    modelFilePath = globals()['train'](training, test, schema)
    
    o = urlparse(os.environ['SHDP_HD_FSWEB'])
    hdfs = webhdfs.createWebHdfs(o.hostname, o.port, pwd.getpwuid(os.getuid())[0])
    modelDirPath = schema["model_data_dir"]
    hdfs.mkdir(modelDirPath)
    hdfsFilePath = stripPath(modelFilePath)
    hdfs.copyFromLocal(modelFilePath, modelDirPath + "/" + hdfsFilePath)
     
    

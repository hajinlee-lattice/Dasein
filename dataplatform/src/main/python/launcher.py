import os
import pwd
import sys
from leframework import argumentparser
from leframework import webhdfs
from urlparse import urlparse


def stripPath(fileName):
    #return fileName
    return fileName[fileName.rfind('/')+1:fileName.__len__()]

if __name__ == "__main__":
    """
    Transform the inputs into python objects and invoke user python script.
    
    Arguments:
    sys.argv[1] -- schema json file
    """

    parser = argumentparser.createParser(stripPath(sys.argv[1]))
    schema = parser.getSchema()
    training = parser.createList(stripPath(schema["training_data"]))
    test = parser.createList(stripPath(schema["test_data"]))
    script = stripPath(schema["python_script"])
    execfile(script)
    
    modelFilePath = globals()['train'](training, test, schema)
    
    o = urlparse(os.environ['SHDP_HD_FSWEB'])
    hdfs = webhdfs.createWebHdfs(o.hostname, o.port, pwd.getpwuid(os.getuid())[0])
    modelDirPath = schema["model_data_dir"]
    hdfs.mkdir(modelDirPath)
    hdfsFilePath = stripPath(modelFilePath)
    hdfs.copyFromLocal(modelFilePath, "%s/%s" % (modelDirPath, hdfsFilePath))
     
    

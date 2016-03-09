import os
import fastavro as avro
from abc import ABCMeta, abstractmethod

class Executor(object):
    '''
    Base class for executing a data processing flow
    '''
    __metaclass__ = ABCMeta

    def __init__(self): pass
    
    @abstractmethod
    def parseData(self, parser, trainingFile, testFile, postProcessClf): pass

    @abstractmethod
    def transformData(self, params): pass

    @abstractmethod
    def postProcessClassifier(self, clf, params): pass

    def loadData(self):
        return True, True
    
    def writeToHdfs(self, hdfs, params):
        # Copy the model data files from local to hdfs
        modelLocalDir = params["modelLocalDir"]
        modelHdfsDir = params["modelHdfsDir"] 
        hdfs.mkdir(modelHdfsDir)
        (_, _, filenames) = os.walk(modelLocalDir).next()
        for filename in filter(lambda e: self.accept(e), filenames):
            hdfs.copyFromLocal(modelLocalDir + filename, "%s%s" % (modelHdfsDir, filename))

    @abstractmethod
    def getModelDirPath(self, schema): pass
    
    @abstractmethod
    def accept(self, filename): pass
    
    def getModelDirByContainerId(self, schema):
        if "CONTAINER_ID" in os.environ:
            tokens = os.environ['CONTAINER_ID'].split("_")
            if(tokens[1].startswith("e")):
                appIdList = tokens[2:4]
            else:
                appIdList = tokens[1:3]
            modelDirPath = "%s/%s" % (schema["model_data_dir"], "_".join(appIdList))
            return modelDirPath
        else:
            return ""
    
    def retrieveMetadata(self, schema, depivoted):
        metadata = dict()
        realColNameToRecord = dict()
        
        if os.path.isfile(schema):
            with open(schema) as fp:
                reader = avro.reader(fp)
                for record in reader:
                    colname = record["barecolumnname"]
                    sqlcolname = ""
                    record["hashValue"] = None
                    if record["Dtype"] == "BND":
                        sqlcolname = colname + "_Continuous"  if depivoted else colname
                    elif depivoted:
                        sqlcolname = colname + "_" + record["columnvalue"]
                    else:
                        sqlcolname = colname
                        record["hashValue"] = record["columnvalue"]
                
                    if colname in metadata:
                        metadata[colname].append(record)
                    else:
                        metadata[colname] = [record]
                
                    realColNameToRecord[sqlcolname] = [record]
        return (metadata, realColNameToRecord)
    
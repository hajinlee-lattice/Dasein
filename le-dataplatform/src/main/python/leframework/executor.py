import os
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
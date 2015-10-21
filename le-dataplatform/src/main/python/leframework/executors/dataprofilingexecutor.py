from leframework.codestyle import overrides
from leframework.executor import Executor

class DataProfilingExecutor(Executor):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
    @overrides(Executor)
    def parseData(self, parser, trainingFile, testFile, postProcessClf):
        training = parser.createList(trainingFile, postProcessClf)
        test = parser.createList(testFile, postProcessClf)
        return (training, test)
    
    @overrides(Executor)
    def transformData(self, params):
        training = params["training"]
        test = params["test"]
        return (training, test, None)
    
    @overrides(Executor)
    def postProcessClassifier(self, clf, params): pass
    
    @overrides(Executor)
    def writeToHdfs(self, hdfs, params):
        super(DataProfilingExecutor, self).writeToHdfs(hdfs, params)

    @overrides(Executor)
    def getModelDirPath(self, schema):
        return schema["model_data_dir"]
    
    @overrides(Executor)
    def accept(self, filename):
        return True
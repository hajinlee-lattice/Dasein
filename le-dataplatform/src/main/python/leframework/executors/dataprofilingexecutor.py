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
    def loadData(self):
        return True, True

    @overrides(Executor)
    def parseData(self, parser, trainingFile, testFile, postProcessClf):
        training = parser.createList(trainingFile, postProcessClf)
        test = parser.createList(testFile, postProcessClf)
        return (training, test)

    @overrides(Executor)
    def transformData(self, params):
        schema = params["schema"]
        samples = schema["samples"]
        features = schema["features"]
        columnsMetadata = []
        if "config_metadata" in schema and schema["config_metadata"] is not None \
                and "Metadata" in schema["config_metadata"] and schema["config_metadata"]["Metadata"] is not None:
            columnsMetadata = schema["config_metadata"]["Metadata"]

        for (key, colname) in samples.iteritems():
            if colname not in features:
                features.append(colname)
            for columnMetadata in [c for c in columnsMetadata]:
                if columnMetadata['ColumnName'] == colname:
                    index = columnsMetadata.index(columnMetadata)
                    del(columnsMetadata[index])

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
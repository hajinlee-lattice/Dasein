from leframework.codestyle import overrides
from leframework.executor import Executor

class DataRuleExecutor(Executor):
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
        test = params["test"]

        metadata = self.retrieveMetadata(params["schema"]["data_profile"], params["parser"].isDepivoted())
        configMetadata = params["schema"]["config_metadata"]["Metadata"] if params["schema"]["config_metadata"] is not None else None
        stringColumns = params["parser"].getStringColumns() - set(params["parser"].getKeys())
        pipelineDriver = params["schema"]["pipeline_driver"]
        pipelineLib = params["schema"]["python_pipeline_lib"]
        pipelineProps = params["schema"]["pipeline_properties"] if "pipeline_properties" in params["schema"] else ""

        # Execute the packaged script from the client and get the returned file
        # that contains the generated data pipeline
        script = params["pipelineScript"]
        execfile(script, globals())

        # Transform the categorical values in the metadata file into numerical values
        globals()["encodeCategoricalColumnsForMetadata"](metadata[0])

        # Create the datarule pipeline
        pipeline = globals()["setupRulePipeline"](pipelineDriver, \
                                                               pipelineLib, \
                                                               metadata[0], \
                                                               stringColumns, \
                                                               params["parser"].target, \
                                                               pipelineProps)
        params["pipeline"] = pipeline
        training = pipeline.apply(params["training"], configMetadata)
        return (training, test, metadata)

    @overrides(Executor)
    def postProcessClassifier(self, clf, params): pass

    @overrides(Executor)
    def writeToHdfs(self, hdfs, params):
        super(DataRuleExecutor, self).writeToHdfs(hdfs, params)

    @overrides(Executor)
    def getModelDirPath(self, schema):
        return schema["model_data_dir"]

    @overrides(Executor)
    def accept(self, filename):
        return True
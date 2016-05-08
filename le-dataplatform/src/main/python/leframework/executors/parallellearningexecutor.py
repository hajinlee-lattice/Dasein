import logging
import os
import pickle
from leframework.codestyle import overrides
from leframework.executor import Executor

logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='parallellearningexecutor')

class ParallelLearningExecutor(Executor):
    '''
    Base executor for creating individual model pickles. Used in parallel mode and invoked by the Mapper. 
    It does basic data preparation before passing the data into the modeling and dumps the pickle file of an individual model.
    '''

    def __init__(self):
        self.pickleFile = "model.p"
        return

    @overrides(Executor)
    def loadData(self):
        return True, False
    
    @overrides(Executor)
    def parseData(self, parser, trainingFile, testFile, postProcessClf):
        training = parser.createList(trainingFile, postProcessClf)
        return (training, None)

    @overrides(Executor)
    def transformData(self, params):
        metadata = self.retrieveMetadata(params["schema"]["data_profile"], params["parser"].isDepivoted())
        configMetadata = params["schema"]["config_metadata"]["Metadata"] if params["schema"]["config_metadata"] is not None else None
        stringColumns = params["parser"].getStringColumns() - set(params["parser"].getKeys())
        pipelineDriver = params["schema"]["pipeline_driver"]
        pipelineLib = params["schema"]["python_pipeline_lib"]

        # Execute the packaged script from the client and get the returned file
        # that contains the generated data pipeline
        script = params["pipelineScript"]
        execfile(script, globals())

        # Transform the categorical values in the metadata file into numerical values
        globals()["encodeCategoricalColumnsForMetadata"](metadata[0])

        # Create the data pipeline
        pipeline, scoringPipeline = globals()["setupPipeline"](pipelineDriver,
                                                               pipelineLib,
                                                               metadata[0],
                                                               stringColumns, 
                                                               params["parser"].target)
        params["pipeline"] = pipeline
        params["scoringPipeline"] = scoringPipeline

        training = pipeline.predict(params["training"], configMetadata, False)

        return (training, None, metadata)

    @overrides(Executor)
    def postProcessClassifier(self, clf, params):
        if clf != None:
            pickle.dump(clf, open(params["modelLocalDir"] + self.pickleFile, "w"), pickle.HIGHEST_PROTOCOL)
        else:
            logger.warn("Generated classifier is null.")
            
    @overrides(Executor)
    def writeToHdfs(self, hdfs, params):
        if os.path.isfile(self.pickleFile):
            hdfs.copyFromLocal(self.pickleFile, "%s%s" % (params["modelHdfsDir"], self.pickleFile))
        super(ParallelLearningExecutor, self).writeToHdfs(hdfs, params)

    @overrides(Executor)
    def getModelDirPath(self, schema):
        return schema["model_data_dir"]

    @overrides(Executor)
    def accept(self, filename):
        badSuffixes = [".dot", ".gz"]

        for badSuffix in badSuffixes:
            if filename.endswith(badSuffix):
                return False
        return True
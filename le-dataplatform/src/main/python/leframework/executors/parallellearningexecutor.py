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
        pipeline, metadata, _ = self.createDataPipeline(params)
        configMetadata = params["schema"]["config_metadata"]["Metadata"] if params["schema"]["config_metadata"] is not None else None

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
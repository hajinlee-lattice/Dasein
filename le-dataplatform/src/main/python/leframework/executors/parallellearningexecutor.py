import logging
import os
import fastavro as avro
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
    
    @overrides(Executor)
    def parseData(self, parser, trainingFile, testFile, postProcessClf):
        training = parser.createList(trainingFile, postProcessClf)
        return (training, None)

    @overrides(Executor)
    def transformData(self, params):
        metadata = self.retrieveMetadata(params["schema"]["data_profile"], params["parser"].isDepivoted())
        stringColumns = params["parser"].getStringColumns()

        # Execute the packaged script from the client and get the returned file
        # that contains the generated data pipeline
        script = params["pipelineScript"]
        execfile(script, globals())

        # Transform the categorical values in the metadata file into numerical values
        globals()["encodeCategoricalColumnsForMetadata"](metadata[0])

        # Create the data pipeline
        pipeline = globals()["setupPipeline"](metadata[0], stringColumns)
        params["pipeline"] = pipeline

        training = pipeline.predict(params["training"])

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
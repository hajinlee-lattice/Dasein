import base64
from collections import OrderedDict
import fnmatch
import gzip
import logging
import os
import pickle

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State
from pipelinefwk import Pipeline


class ModelGenerator(State, JsonGenBase):

    def __init__(self):
        State.__init__(self, "ModelGenerator")
        self.logger = logging.getLogger(name = 'modelgenerator')


    @overrides(State)
    def execute(self):
        mediator = self.mediator

        model = OrderedDict()
        model["__type"] = "PythonScriptModel:#LatticeEngines.DataBroker.ServiceInterface"
        model["AdjustmentFactor"] = 1
        model["ColumnMetadata"] = None
        model["InitialTransforms"] = None
        model["Target"] = 1

        mediator.modelType = model["__type"]

        version = self.__getLatticeVersion()
        if version is not None:
            model["LatticeVersion"] = version

        randomSeed = self.__getRandomSeed()
        if randomSeed is not None:
            model["RandomSeed"] = randomSeed

        with open("leframework.tar.gz/leframework/scoringengine.py", "r") as pythonFile:
            model["Script"] = "".join(pythonFile.readlines())

        filename = mediator.modelLocalDir + '/STPipelineBinary.p'
        pipeline = self.__getPipeline(mediator)
        mediator.pipeline = pipeline
        pickle.dump(pipeline, open(filename, "w"), pickle.HIGHEST_PROTOCOL)
        filename = self.__compressFile(filename)
        pipelineFwkPkl = self.__getSerializedFile(self.__compressFile("pipelinefwk.py"))
        pipelineBinaryPkl = self.__getSerializedFile(filename)
        pipelinePkl = self.__getSerializedFile(self.__compressFile(self.mediator.schema["python_pipeline_script"]))
        model["CompressedSupportFiles"] = [{ "Value": pipelinePkl, "Key": self.mediator.schema["python_pipeline_script"] },
                                           { "Value": pipelineBinaryPkl, "Key": "STPipelineBinary.p" },
                                           { "Value": pipelineFwkPkl, "Key": "pipelinefwk.py" }]

        (dirpath, _, filenames) = os.walk(self.mediator.schema["python_pipeline_lib"]).next()

        filenames = sorted(filenames)
        for filename in filenames:
            if filename.endswith(".pyc") or filename.endswith(".gz"):
                continue
            filePkl = self.__getSerializedFile(self.__compressFile(dirpath + "/" + filename))
            if filename not in [f['Key'] for f in model["CompressedSupportFiles"]]:
                model["CompressedSupportFiles"].append({ "Value": filePkl, "Key": filename })

        for step in pipeline.getPipeline():
            rtsArtifacts = step.getRTSArtifacts()

            if len(rtsArtifacts) == 0:
                continue
            for key, filePath in rtsArtifacts:
                if filePath is not None:
                    filePkl = self.__getSerializedFile(self.__compressFile(filePath))
                    if key not in [f['Key'] for f in model["CompressedSupportFiles"]]:
                        model["CompressedSupportFiles"].append({ "Value": filePkl, "Key": key })

        swlibDir = self.__getSwlibPath()

        if swlibDir is not None:
            try:
                files = []
                for root, _, filenames in os.walk(swlibDir):
                    for filename in fnmatch.filter(filenames, "*.py"):
                        files.append((root, filename))
                for dirpath, filename in files:
                    if filename != "__init__.py":
                        filePkl = self.__getSerializedFile(self.__compressFile(dirpath + "/" + filename))
                        if filename not in [f['Key'] for f in model["CompressedSupportFiles"]]:
                            model["CompressedSupportFiles"].append({ "Value": filePkl, "Key": filename })
            except Exception as e:
                self.logger.warn(str(e));

        self.model = model

    def __getRandomSeed(self):
        if "random_state" in self.mediator.algorithmProperties:
            return self.mediator.algorithmProperties["random_state"]
        return None

    def __getLatticeVersion(self):
        provenanceProperties = self.mediator.provenanceProperties
        if "Lattice_Version" in provenanceProperties:
            return provenanceProperties["Lattice_Version"]
        return None

    def __getSwlibPath(self):
        provenanceProperties = self.mediator.provenanceProperties
        artifactId = None
        version = None
        if "swlib.artifact_id" in provenanceProperties:
            artifactId = provenanceProperties["swlib.artifact_id"]
        if "swlib.version" in provenanceProperties:
            version = provenanceProperties["swlib.version"]

        if artifactId is not None and version is not None:
            return "%s-%s.jar" % (artifactId, version)
        return None


    def __getPipeline(self, mediator):
        scoringPipeline = mediator.scoringPipeline
        pipelineSteps = scoringPipeline.getPipeline()
        steps = []
        for step in pipelineSteps:
            if step.isModelStep():
                newStep = step.clone(mediator.clf, mediator.schema["features"], mediator.revenueColumn)
                steps.append(newStep)
                continue
            steps.append(step)
        return Pipeline(steps)

    def __compressFile(self, filename):
        with open(filename, "rb") as uncompressedFile:
            with gzip.open(filename + ".gz", "wb", compresslevel=6) as compressedFile:
                compressedFile.write(uncompressedFile.read())
        return compressedFile.name


    def __getSerializedFile(self, filename):
        return base64.b64encode(bytearray(open(filename, "rb").read()))

    @overrides(JsonGenBase)
    def getKey(self):
        return "Model"

    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.model

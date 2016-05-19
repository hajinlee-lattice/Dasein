import base64
import gzip

from pipelinefwk import Pipeline


class ModelUtil():

    @staticmethod
    def getPipeline(mediator):
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
    
    @staticmethod
    def compressFile(filename):
        with open(filename, "rb") as uncompressedFile:
            with gzip.open(filename + ".gz", "wb", compresslevel=6) as compressedFile:
                compressedFile.write(uncompressedFile.read())
        return compressedFile.name

    @staticmethod
    def getSerializedFile(filename):
        return base64.b64encode(bytearray(open(filename, "rb").read()))
    

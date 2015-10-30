import logging

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State

class NormalizationGenerator(State, JsonGenBase):
    def __init__(self):
        State.__init__(self, "NormalizationGenerator")
        self.logger = logging.getLogger(name='normalizationgenerator')

    @overrides(State)
    def execute(self):
        mediator = self.mediator
        schema = mediator.schema
        score = mediator.allDataPreTransform[schema["reserved"]["score"]]
        self.mediator.mappingData = []

        if score is not None:
            mappingData = self.buildMappingFunctionData(score)
            self.mediator.mappingData = mappingData
        else:
            self.logger.info("Scores are not provided. Normalization buckets cannot be generated.")

    def buildMappingFunctionData(self, probabilityList):
        bins = self.createIndexSequence(len(probabilityList), 100)
        orderedProb = sorted(probabilityList, reverse = False)
        cumulativePercent = [1.0 * bins[i + 1] / bins[len(bins) - 1] for i in range(len(bins) - 1)]

        startProb = [orderedProb[bins[i]] for i in range(len(bins) - 1)]
        endProb = startProb[:]
        del endProb[0]
        maxProb = orderedProb[len(probabilityList)-1]
        endProb.append(maxProb)

        return[{"StartProbability" : startProb[i], "EndProbability" : endProb[i], "CumulativePercentage" : cumulativePercent[i]} for i in range(len(startProb))]

    def createIndexSequence(self, number, rawSplits):
        binSize = int(number / (rawSplits+1) / 1.0)
        numBins=int(number / binSize)

        sp=[binSize * i for i in range(numBins + 1)]
        sp[numBins] = number
        minSize = min(sp[i + 1] - sp[i] for i in range(len(sp) - 1))
        if minSize < 2:
            sp = self.createIndexSequence(number, rawSplits - 2)
        return tuple(sp)

    @overrides(JsonGenBase)
    def getKey(self):
        return "NormalizationBuckets"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.mediator.mappingData

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
        self.mediator.probabilityMappingData = []
        self.mediator.revenueMappingData = []
 
        if mediator.revenueColumn is not None:
            revenue = mediator.data[mediator.schema["reserved"]["expectedrevenue"]]
            revenueMappingData = self.buildMappingFunctionData(revenue)
            self.mediator.revenueMappingData = revenueMappingData
             
        if score is not None:
            mappingData = self.buildMappingFunctionData(score)
            self.mediator.probabilityMappingData = mappingData
        else:
            self.logger.info("Scores are not provided. Normalization buckets cannot be generated.")
 
    # score list contains probabilities (probability model) or revenue (revenue model) values
    def buildMappingFunctionData(self, scoreList):
        bins = self.__createIndexSequence(len(scoreList), min((len(scoreList)-1), 100))
         
        if len(bins) > 1:
            orderedScores = sorted(scoreList, reverse = False)
            cumulativePercent = [1.0 * bins[i + 1] / bins[len(bins) - 1] for i in range(len(bins) - 1)]
     
            startScore = [orderedScores[bins[i]] for i in range(len(bins) - 1)]
            endScore = startScore[:]
            del endScore[0]
            maxScore = orderedScores[len(scoreList) - 1]
            endScore.append(maxScore)
 
        return[{"Start" : startScore[i], "End" : endScore[i], "CumulativePercentage" : cumulativePercent[i]} for i in range(len(startScore))]
 
 
    def __createIndexSequence(self, number, rawSplits):
        if number == 0:
            return []
         
        binSize = int(number / (rawSplits + 1) / 1.0)
         
        if binSize < 1:
            binSize = 1
             
        numBins = int(number / binSize)
 
        sp=[binSize * i for i in range(numBins + 1)]
        sp[numBins] = number
        minSize = min(sp[i + 1] - sp[i] for i in range(len(sp) - 1))
        if minSize < 2:
            sp = self.__createIndexSequence(number, rawSplits - 2)
        return tuple(sp)
 
    @overrides(JsonGenBase)
    def getKey(self):
        return "NormalizationBuckets"
     
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        normalizationBuckets = {"Probability" : self.mediator.probabilityMappingData}
         
        if self.mediator.revenueColumn is not None:
            normalizationBuckets["ExpectedRevenue"] = self.mediator.revenueMappingData          
        return normalizationBuckets 
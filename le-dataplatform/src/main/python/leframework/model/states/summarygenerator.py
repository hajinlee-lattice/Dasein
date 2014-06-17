from collections import OrderedDict
import logging
from sklearn import metrics
from sklearn.metrics.cluster.supervised import entropy
import uuid

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State

class SummaryGenerator(State, JsonGenBase):
    
    def __init__(self):
        State.__init__(self, "SummaryGenerator")
        self.logger = logging.getLogger(name='summarygenerator')
    
    @overrides(State)
    def execute(self):
        mediator = self.mediator
        self.summary = OrderedDict()
        predictors = []
        eventData = mediator.data[:, mediator.schema["targetIndex"]]
        for key, value in mediator.metadata[0].iteritems():
            if key + "_1" in mediator.schema["targets"]:
                continue
            self.logger.info("Generating predictors for " + key)
            predictors.append(self.generatePredictors(key, value, eventData))
        
        # Sort predictor by UncertaintyCoefficient
        predictors = sorted(predictors, key = lambda x: x["UncertaintyCoefficient"], reverse=True)
        self.summary["Predictors"] = predictors
        self.summary["RocScore"] = self.__getRocScore(zip(self.mediator.scored, self.mediator.target))
        self.summary["SegmentChart"] = self.__getSegmentChart(mediator.probRange, mediator.widthRange, mediator.buckets, mediator.averageProbability)
    
    @overrides(JsonGenBase)
    def getKey(self):
        return "Summary"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.summary
    
    def __getCountWhereEventIsOne(self, predictorData, eventData):
        counter = lambda x,y: 1 if x == 1 and y == 1 else 0
        return sum(map(counter, predictorData, eventData))
    
    def generatePredictors(self, colname, metadata, eventData):
        elements = []

        attrLevelUncertaintyCoeff = 0
        for record in metadata:
            self.logger.info(record)
            predictorData = self.__getPredictorVector(colname, record)
            element = OrderedDict()
            
            countForBandValue = sum(predictorData)
            
            # If a band value is not found, skip that predictor value
            if countForBandValue == 0:
                self.logger.critical("No data found in the test set for this band or value.")
                continue
            
            countForBandValueAndEventIsOne = self.__getCountWhereEventIsOne(predictorData, eventData) #TODO
            lift = float(countForBandValueAndEventIsOne)/float(countForBandValue) # TODO
            avgLift = float(sum(eventData))/float(len(eventData))
            element["CorrelationSign"] = 1 if lift > avgLift else -1
            element["Count"] = countForBandValue
            if record["Dtype"] == "BND":
                element["LowerInclusive"] = record["minV"]
            element["Name"] = str(uuid.uuid4())
            element["UncertaintyCoefficient"] = self.__uncertaintyCoefficientXgivenY(eventData, predictorData)
            attrLevelUncertaintyCoeff += element["UncertaintyCoefficient"]
            if record["Dtype"] == "BND":
                element["UpperExclusive"] = record["maxV"]
            if record["Dtype"] == "BND":
                element["Value"] = None
            else:
                element["Value"] = record["columnvalue"]
            elements.append(element)
        
        # Sort elements by UncertaintyCoefficient
        elements = sorted(elements, key = lambda x: x["UncertaintyCoefficient"], reverse=True)
        predictor = OrderedDict()
        predictor["Elements"] = elements
        predictor["Name"] = colname
        predictor["UncertaintyCoefficient"] = attrLevelUncertaintyCoeff
        return predictor
    
    def __getPredictorVector(self, colname, record):
        converter = None
        try:
            if record["Dtype"] == "BND":
                newColName = colname + "_Continuous" if self.mediator.depivoted else colname
                columnData = self.mediator.data[:, self.mediator.schema["nameToFeatureIndex"][newColName]]
                minV = record["minV"]
                maxV = record["maxV"]
                converter = lambda x: 1 if x >= minV and x < maxV else 0
                return map(converter, columnData)
            elif self.mediator.depivoted:                
                return self.mediator.data[:, self.mediator.schema["nameToFeatureIndex"][colname + "_" + record["columnvalue"]]]
            else:
                columnData = self.mediator.data[:, self.mediator.schema["nameToFeatureIndex"][colname]]
                converter = lambda x: 1 if x == record["hashValue"] else 0
                return map(converter, columnData)
        except:
            return self.mediator.data[:, 1]
        
        
    def __uncertaintyCoefficientXgivenY(self, x, y):
        '''
          Given y, what parts of x can we predict.
          In this case, x should be the event column, while y should be the predictor column-value
        '''
        return metrics.mutual_info_score(x, y)/entropy(x) 

    def __getSegmentChart(self, probRange, widthRange, buckets, averageProbability):
        # Generate inclusive (min,max) with highest max = null and lowest min = 0
        if len(probRange) == 1:
            inclusive = [(0,None)]
        else:
            inclusive = [((probRange[0]+probRange[1])/2,None)]         
            for i in range (1,len(probRange)-1):
                inclusive.append(((probRange[i]+probRange[i+1])/2,inclusive[i-1][0]))
            inclusive.append((0,inclusive[len(probRange)-2][0]))
                  
        # Generate name for each segment
        names = []
        for i in range(len(probRange)):
            curProb = probRange[i] if self.mediator.type == 0 else probRange[i]/averageProbability
            for j in range(len(buckets)): 
                if buckets[j]["Minimum"] is not None and buckets[j]["Maximum"] is not None:
                    if curProb >= buckets[j]["Minimum"] and curProb < buckets[j]["Maximum"]:
                        names.append(buckets[j]["Name"])
                        break
                elif buckets[j]["Minimum"] is not None and curProb >= buckets[j]["Minimum"]:
                        names.append(buckets[j]["Name"])
                        break
                elif buckets[j]["Maximum"] is not None and curProb < buckets[j]["Maximum"]:    
                        names.append(buckets[j]["Name"])
                        break
                            
        # Generate segments
        segments = []
        for i in range(len(probRange)):
            element = OrderedDict()
            element["AverageProbability"] = probRange[i]
            element["LowerInclusive"] = inclusive[i][0]
            element["Name"] = names[i]
            element["UpperExclusive"] = inclusive[i][1]
            element["Width"] = widthRange[i]
            segments.append(element)
     
        # Generate segment chart
        segmentChart = OrderedDict()
        segmentChart["AverageProbability"] = averageProbability
        segmentChart["Segments"] = segments
        
        return segmentChart

    def __getRocScore(self,score):
        # Sort by target
        score.sort(key=lambda rowScore: (rowScore[1], rowScore[0]), reverse=True)
        theoreticalBestCounter = 0
        theoreticalBestArea = 0
        for i in range(len(score)):
            theoreticalBestCounter += score[i][1]
            theoreticalBestArea += theoreticalBestCounter
        
        # Sort by score
        score.sort(key=lambda rowScore: (rowScore[0], rowScore[1]), reverse=True)
        actualBestCounter = 0
        actualBestArea = 0
        for i in range(len(score)):
            actualBestCounter += score[i][1]
            actualBestArea += actualBestCounter
        
        return actualBestArea/float(theoreticalBestArea)
        
        
        
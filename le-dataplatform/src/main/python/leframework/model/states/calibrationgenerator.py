from collections import OrderedDict
import logging
import math

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class CalibrationGenerator(State, JsonGenBase):

    def __init__(self):
        State.__init__(self, "CalibrationGenerator")
        self.logger = logging.getLogger(name='calibrationgenerator')
    
    @overrides(State)
    def execute(self):
        mediator = self.mediator
        schema = mediator.schema

        orderedScore = self.mediator.data[[schema["reserved"]["score"], schema["target"]]]
        orderedScore.sort([schema["reserved"]["score"], schema["target"]], axis=0, ascending=False, inplace=True)
            
        # get test size and range width 
        numTest = orderedScore.shape[0]
        
        algorithmProperties = self.mediator.algorithmProperties
        
        calibrationWidth = 1.0
        if "calibration_width" in algorithmProperties:
            calibrationWidth = float(algorithmProperties["calibration_width"])
            
        defaultRangeWidth = int(math.ceil(float(numTest) / 100)) * calibrationWidth
        self.logger.info("Generating calibration with test size: %s range width: %s" % (str(numTest), str(defaultRangeWidth)))
        
        # calculate cumulative target count, sum of targets from 0 to i 
        cumulativeCount = [0] * (numTest + 1)  # add a sentinel at index 0
        cumulativeCount[0] = 0  # sentinel = 0
        for i in xrange(numTest):
            cumulativeCount[i + 1] = cumulativeCount[i] + orderedScore.iloc[i][1]
        
        # calculate the average conversion probability
        self.averageProbability = cumulativeCount[numTest] / float(numTest)
        
        # distinct score count, a list of (score, count)
        distinctScoreCount = []
        curScore = orderedScore.iloc[0][0] 
        count = 0
        for i in xrange(numTest):
            tmpScore = orderedScore.iloc[i][0]
            if curScore != tmpScore:
                distinctScoreCount.append((curScore, count))
                curScore = tmpScore
                count = 1  # reset and count new score
            else:
                count += 1
            if i == numTest - 1:
                distinctScoreCount.append((curScore, count))  # include the last score
        
        # default calibration range, list of (max,min)        
        calibrationRange = []
        indexRange = []  # stores corresponding index value 
        curMax = None  # first range's max is null
        curWidth = 0
        curIdx = 1
        for i in xrange(len(distinctScoreCount)):
            tmpScoreCount = distinctScoreCount[i]
            # end of current calibration range
            if curWidth + tmpScoreCount[1] >= defaultRangeWidth or i == len(distinctScoreCount) - 1:
                calibrationRange.append((curMax, tmpScoreCount[0]))
                indexRange.append((curIdx, curIdx + curWidth + tmpScoreCount[1] - 1))
                curIdx = curIdx + curWidth + tmpScoreCount[1]
                curMax = tmpScoreCount[0]
                curWidth = 0
            else:
                curWidth += tmpScoreCount[1]
      
        # set last range's min to null 
        oldRange = calibrationRange.pop(len(calibrationRange) - 1) 
        calibrationRange.append((oldRange[0], None))
        self.logger.info("number of calibration ranges before: %s" % str(len(calibrationRange)))
        # merge calibration ranges
        hasConflict = True 
        while (hasConflict):
            # loop the entire ranges
            idx = 0
            hasConflict = False
            while (idx < len(indexRange) - 1):  
                if self.__needMerge(indexRange[idx], indexRange[idx + 1], cumulativeCount):
                    hasConflict = True
                    self.__merge(calibrationRange, indexRange, idx)
                else:
                    idx += 1

        # calculate probability and width for each range
        probRange = self.__getProbRange(indexRange, cumulativeCount)
        widthRange = self.__getWidthRange(indexRange)
        # prepare calibration with specified format
        self.calibration = []
        self.logger.info("number of calibration ranges generated: " + str(len(probRange))) 
        for i in xrange(len(calibrationRange)):
            element = OrderedDict()
            element["MaximumScore"] = calibrationRange[i][0]
            element["MinimumScore"] = calibrationRange[i][1]
            element["Probability"] = probRange[i]
            element["Width"] = widthRange[i]
            self.calibration.append(element)
            
        # pass parameters to mediator
        self.mediator.probRange = probRange
        self.mediator.widthRange = widthRange
        self.mediator.averageProbability = self.averageProbability
        self.mediator.calibration = self.calibration
        
    @overrides(JsonGenBase)
    def getKey(self):
        return "Calibration"
    
    @overrides(JsonGenBase)
    def getJsonProperty(self):
        return self.calibration

    ####  Helper functions #####
    def __needMerge(self, range1, range2, cumulateCount):
        prob1 = self.__getProb(range1[0], range1[1], cumulateCount)
        prob2 = self.__getProb(range2[0], range2[1], cumulateCount)
        return prob1 <= prob2;

    def __merge(self, calibrationRange, indexRange, idx):
        if idx < 0 or idx >= len(indexRange) - 1:
            self.logger.error("Index out of bound to merge: "+str(idx)) 
            return
        # merge index range
        newRange = (indexRange[idx][0], indexRange[idx + 1][1])
        indexRange.pop(idx)
        indexRange.pop(idx)
        indexRange.insert(idx, newRange)
    
        # merge calibration range
        newRange = (calibrationRange[idx][0], calibrationRange[idx + 1][1])
        calibrationRange.pop(idx)
        calibrationRange.pop(idx)
        calibrationRange.insert(idx, newRange)

    def __getProb(self, lo, hi, cumulateCount):
        return (cumulateCount[hi] - cumulateCount[lo - 1]) / float(hi - lo + 1) 

    def __getProbRange(self, indexRange, cumulateCount):
        probRange = []
        for i in xrange(len(indexRange)):
            probRange.append(self.__getProb(indexRange[i][0], indexRange[i][1], cumulateCount))
    
        return probRange
    
    def __getWidthRange(self,indexRange):
        widthRange = [] 
        for i in xrange(len(indexRange)):
            widthRange.append(indexRange[i][1]-indexRange[i][0]+1)
    
        return widthRange

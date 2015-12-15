from collections import OrderedDict
import logging
import math

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State
from collections import Counter

startingNumberBuckets = 100
minimumBucketPopulation = 100
minLiftDifference = 0.2
minProbDifference = 0.1

minBucketWidth = .04
minNumberBuckets = 8
maxNumberBuckets = 20
useLiftAndProbDifference = False


class CalibrationWithWidthGenerator(State, JsonGenBase):

    def __init__(self):
        State.__init__(self, "CalibrationGenerator")
        self.logger = logging.getLogger(name='calibrationgenerator')
    
    @overrides(State)
    def execute(self):
        mediator = self.mediator
        schema = mediator.schema

        orderedScore = self.mediator.data[[schema["reserved"]["score"], schema["target"]]]
        orderedScore.sort([schema["reserved"]["score"], schema["target"]], axis=0, ascending=False, inplace=True)

        # convert pd array into simple lists
        score = list(orderedScore.iloc[:, 0].values)
        target = list(orderedScore.iloc[:, 1].values)
        # get test size and range width 
        numTest = len(score)
        
        defaultRangeWidth = max(int(math.ceil(float(numTest) / startingNumberBuckets)), minimumBucketPopulation)
        self.logger.info("Generating calibration with test size: %s range width: %s" % (str(numTest), str(defaultRangeWidth)))

        cumulativeCount = reduce(lambda x, y: x + [x[-1] + y], target, [0])[1:]
        
        # calculate the average conversion probability
        self.averageProbability = sum(target) / float(len(target))

        # distinct score count, a list of (score, count)
        distinctCount = sorted(Counter(score).items(), key=lambda x: x[0], reverse=True)
        
        # default calibration range, list of (max,min)
        # populated calibrationRange, indexRange
        calibrationRange = []
        indexRange = []  # stores corresponding index value 
        curMax = None  # first range's max is null
        curWidth = 0
        curIdx = 0
        for i in range(len(distinctCount)):
            tempScore, tempCount = distinctCount[i]
            # end of current calibration range
            if curWidth + tempCount >= defaultRangeWidth or i == len(distinctCount) - 1:
                calibrationRange.append((curMax, tempScore))
                indexRange.append((curIdx, curIdx + curWidth + tempCount - 1))
                curIdx = curIdx + curWidth + tempCount
                curMax = tempScore
                curWidth = 0
            else:
                curWidth += tempCount
      
        # set last range's min to null 
        oldRange = calibrationRange.pop(len(calibrationRange) - 1) 
        calibrationRange.append((oldRange[0], None))
        self.logger.info("number of calibration ranges before: %s" % str(len(calibrationRange)))

        # merge based on size
        initialLength = len(indexRange)
        minBucketWidthUse = minBucketWidth
        while len(indexRange) > minNumberBuckets:
            indexSize = [(x[1] - x[0]) for x in indexRange]
            minSize = min(indexSize)
            if minSize >= minBucketWidthUse * numTest:
                if len(indexRange) > maxNumberBuckets:
                    minBucketWidthUse = .005 + 1.0 / maxNumberBuckets
                else:
                    break
            ix = indexSize.index(minSize)
            if ix == len(indexSize) - 1:
                mergeDirection = -1
            elif  ix == 0:
                mergeDirection = 0
            else:
                if indexSize[ix + 1] <= indexSize[ix - 1]:
                    mergeDirection = 0
                else:
                    mergeDirection = -1
            self.__merge(calibrationRange, indexRange, mergeDirection + ix)
        
    
     
        # merge calibration ranges
        hasConflict = True 
        while (hasConflict):
            # loop the entire ranges
            idx = len(indexRange) - 2
            if idx < 0: break
            hasConflict = False
            while idx >= 0:  
                if self.__needMerge(indexRange[idx], indexRange[idx + 1], cumulativeCount):
                    hasConflict = True
                    self.__merge(calibrationRange, indexRange, idx)
                idx -= 1 
                
        firstIndexSize = indexRange[0][1] - indexRange[0][0]
        if firstIndexSize <= minBucketWidth * numTest:
            self.__merge(calibrationRange, indexRange, 0)

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
        mult = int(useLiftAndProbDifference)
        prob1 = self.__getProb(range1[0], range1[1], cumulateCount)
        prob2 = self.__getProb(range2[0], range2[1], cumulateCount)
        if prob1 <= prob2 + mult * minProbDifference:
            return True
        lift1 = self.__getLift(range1[1], cumulateCount)
        lift2 = self.__getLift(range2[1], cumulateCount)
        return lift1 <= lift2 + mult * minLiftDifference


    def __merge(self, calibrationRange, indexRange, idx):
        if idx < 0 or idx >= len(indexRange) - 1:
            self.logger.error("Index out of bound to merge: " + str(idx)) 
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
        if lo == 0:  return cumulateCount[hi] / float(hi)
        return (cumulateCount[hi] - cumulateCount[lo - 1]) / float(hi - lo + 1)

    def __getLift(self, hi, cumulateCount):
        return cumulateCount[hi] / float(hi) / self.averageProbability

    
    def __getProbRange(self, indexRange, cumulateCount):
        probRange = []
        for i in xrange(len(indexRange)):
            probRange.append(self.__getProb(indexRange[i][0], indexRange[i][1], cumulateCount))
    
        return probRange
    
    def __getWidthRange(self, indexRange):
        widthRange = [] 
        for i in xrange(len(indexRange)):
            widthRange.append(indexRange[i][1] - indexRange[i][0] + 1)
    
        return widthRange

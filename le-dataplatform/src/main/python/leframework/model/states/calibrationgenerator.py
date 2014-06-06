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
        self.calibration = []   
        scored = self.mediator.scored
        target = self.mediator.target

        # reconstruct the array into a list of (prob, target)
        orderedScore = []
        for i in range(len(scored)):
            orderedScore.append((scored[i][1], int(target[i])))
        orderedScore.sort(key=lambda score: (score[0], score[1]), reverse=True)
        open('test.txt', 'w').write('\n'.join('%s %s' % x for x in orderedScore))  
            
        # get test size and range width 
        numTest = len(orderedScore)                 
        defaultRangeWidth = int(math.ceil(float(numTest) / 100))
        print "test size: "+str(numTest)
        
        # calculate cumulative target count, sum of targets from 0 to i 
        cumulativeCount = [0] * (numTest + 1)  # add a sentinel at index 0
        cumulativeCount[0] = 0  # sentinel = 0
        for i in range(numTest):
            cumulativeCount[i + 1] = cumulativeCount[i] + orderedScore[i][1]
        
        # calculate the average conversion probability
        self.averageProbability = cumulativeCount[numTest] / float(numTest)
        
        # distinct score count, a list of (score, count)
        distinctScoreCount = []
        curScore = orderedScore[0][0] 
        count = 0
        for i in range(numTest):
            tmpScore = orderedScore[i][0]
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
        for i in range(len(distinctScoreCount)):
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

        # calculate probability for each range
        probRange = self.__getProbRange(indexRange, cumulativeCount)
        # prepare calibration with specified format
        for i in range(len(calibrationRange)):
            element = OrderedDict()
            element["MaximumScore"] = calibrationRange[i][0]
            element["MinimumScore"] = calibrationRange[i][1]
            element["Probability"] = probRange[i]
            element["Width"] = indexRange[i][1] - indexRange[i][0] + 1
            self.calibration.append(element)
            
        # pass parameters to mediator
        self.mediator.probRange = probRange
        self.mediator.indexRange = indexRange
        self.mediator.averageProbability = self.averageProbability
        
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
            print "Invalid index to merge!"
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
        for i in range(len(indexRange)):
            probRange.append(self.__getProb(indexRange[i][0], indexRange[i][1], cumulateCount))
    
        return probRange

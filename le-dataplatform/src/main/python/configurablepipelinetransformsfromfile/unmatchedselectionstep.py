'''
Description:

    This step will select unmatched records <= 5% from the data frame
'''
from sklearn import ensemble

from pipelinefwk import PipelineStep
from pipelinefwk import get_logger
import random
import numpy as np
from pandas import DataFrame

logger = get_logger("pipeline")

class UnmatchedSelectionStep(PipelineStep):

    params = {}
    unmatchedRatioThreshold = 0.05
    selectionLift = 0.7

    def __init__(self, params, unmatchedRatioThreshold=0.05, selectionLift=0.7):
        self.params = params
        self.unmatchedRatioThreshold = unmatchedRatioThreshold
        self.selectionLift = selectionLift

    def transform(self, dataFrame, configMetadata, test):
        if test:
            return dataFrame
        
        lidColName = '__LDC_LID__'
        if lidColName not in dataFrame.columns.values:
            return dataFrame
        recordCount = dataFrame.shape[0]
        matchCounter = lambda x: 0 if np.isnan(x) else 1
        matchedCount = sum(map(matchCounter, dataFrame[lidColName]))
        unmatchedRatio = 1.0 - (matchedCount * 1.0 / recordCount)
        if unmatchedRatio <= self.unmatchedRatioThreshold:
            logger.info("Do not do unmatched selection, Total record count:" + str(recordCount) + " Unmatched ratio:" + str(unmatchedRatio))
            return dataFrame

        logger.info("Doing unmatched selection.")
        logger.info("Total record count:" + str(recordCount) + " Matched count:" + str(matchedCount) + " Unmatched ratio:" + str(unmatchedRatio))

        unmatchedIndex = dataFrame[dataFrame[lidColName].isnull()].index
        selectedUnmatchedCount = matchedCount * self.unmatchedRatioThreshold * 1.0 / (1.0 - self.unmatchedRatioThreshold)
        selectedUnmatchedIndex = random.sample(unmatchedIndex, int(selectedUnmatchedCount))
        
        targetColumn = self.params["schema"]["target"]
        eventList = dataFrame[targetColumn].tolist()
        avgConvRate = sum(eventList) * 1.0 / recordCount

        dataFrame[targetColumn][unmatchedIndex] = 0
        unmatchedEventCount = int(len(selectedUnmatchedIndex) * self.selectionLift * avgConvRate)
        if unmatchedEventCount > 0:  
            dataFrame[targetColumn][selectedUnmatchedIndex[:unmatchedEventCount]] = 1

        trainingCol = "__TRAINING__"
        dataFrame[trainingCol] = dataFrame[lidColName].apply(matchCounter) 
        dataFrame[trainingCol][selectedUnmatchedIndex] = 1
         
        logger.info("Selected total record count:" + str(matchedCount + len(selectedUnmatchedIndex)) 
                    + " Matched record count:" + str(matchedCount) 
                    + " Selected unmatched record count:" + str(len(selectedUnmatchedIndex)))
         
        return dataFrame                

    def getOutputColumns(self):
        return []

    def getRTSMainModule(self):
        return ""

    def getRTSArtifacts(self):
        return []

    def doColumnCheck(self):
        return False

    def getDebugArtifacts(self):
        return []

    def includeInScoringPipeline(self):
        return False

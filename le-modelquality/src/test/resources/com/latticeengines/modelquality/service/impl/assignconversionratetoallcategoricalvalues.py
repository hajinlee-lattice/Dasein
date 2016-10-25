'''
Description:
If the choice is made to assign a categorical variable to numeric values, the choice should be the conversion rate for that specitic categorical value.
For example, category A has 0% conversion, B has 1% conversion, and C has 8% conversion in the training set.  Reassign A,B, and C to 0,.01 and .08, respectively
'''
import os
from pipelinefwk import PipelineStep
from pipelinefwk import create_column
from pipelinefwk import get_logger


logger = get_logger("pipeline")

class AssignConversionRateToAllCategoricalValues(PipelineStep):

    columnList = []
    categoricalColumnMapping = None
    categoricalColumnMappingFilePath = None
    totalPositiveThreshold = 12
    columnsToTransform = {}
    targetColumn = ""

    def __init__(self, columnsToTransform, targetColumn, totalPositiveThreshold=12):
        self.columnsToTransform = columnsToTransform
        self.targetColumn = targetColumn
        self.totalPositiveThreshold = totalPositiveThreshold
        
        if self.categoricalColumnMapping is None:
            self.categoricalColumnMapping = {}
        if columnsToTransform:
            self.columnList = columnsToTransform.keys()
        else:
            self.columnList = []
        logger.info("Initialized AssignConversionRate with columnsToTransform " + str(columnsToTransform)
                    + ", targetColumn=" + str(targetColumn)
                    + ", positiveThreshold=" + str(self.totalPositiveThreshold))
    
    def learnParameters(self, trainingDataFrame, testDataFrame, configMetadata):
        for column, _ in self.columnsToTransform.iteritems():
            if column in trainingDataFrame.columns:
                self.__assignConversionRateToCategoricalColumns(column, trainingDataFrame, self.targetColumn)
                logger.info("self.assignConversionRateMapping " + str(self.categoricalColumnMapping))
        self.__writeRTSArtifacts()

    def transform(self, dataFrame, configMetadata, test):
        for column, _ in self.columnsToTransform.iteritems():
            if column in dataFrame.columns:
                self.currentColumn = column
                dataFrame[column] = dataFrame[column].apply(self.__applyConversionRate)
                logger.info("self.assignConversionRateMapping " + str(self.categoricalColumnMapping))
        return dataFrame

    def __assignConversionRateToCategoricalColumns(self, column, dataFrame, targetColumn):
        if self.targetColumn in dataFrame and len(dataFrame[column]) == len(dataFrame[self.targetColumn]):
            logger.info("AssignConversionRate training phase. Converting column: " + column)
            allKeys = set(dataFrame[column])
            listOfValues = [x for x in enumerate(dataFrame[column])]
            totalPositiveEventCount = float(sum(dataFrame[self.targetColumn]))
            meanConversionRate = round(totalPositiveEventCount / (len(dataFrame[self.targetColumn])), 2)
            keyConversionRate = {}
            for key in allKeys:
                ind = [i for i, x in listOfValues if x == key]
                count = len(ind)
                if count > 0:
                    totalPositives = sum([dataFrame[self.targetColumn].iloc[i] for i in ind])
                    if totalPositives >= self.totalPositiveThreshold:
                        perc = round(sum([dataFrame[self.targetColumn].iloc[i] for i in ind]) * 1.0 / count, 2)
                        keyConversionRate[key] = perc
                    else:
                        keyConversionRate[key] = meanConversionRate
                else:
                    keyConversionRate[key] = 0.0
            keyConversionRate["UNKNOWN"] = meanConversionRate
            self.categoricalColumnMapping[column] = keyConversionRate

    def __applyConversionRate(self, categoryValue):
        if self.currentColumn in self.categoricalColumnMapping:
            if categoryValue in self.categoricalColumnMapping[self.currentColumn]:
                if self.categoricalColumnMapping[self.currentColumn][categoryValue]:
                    return self.categoricalColumnMapping[self.currentColumn][categoryValue]
                else:
                    return self.categoricalColumnMapping[self.currentColumn]["UNKNOWN"]
        return categoryValue

    def __writeRTSArtifacts(self):
        with open("conversionratemapping.txt", "w") as fp:
            fp.write(str(self.categoricalColumnMapping))
            self.categoricalColumnMappingFilePath = os.path.abspath(fp.name)

    def getRTSArtifacts(self):
        return [("conversionratemapping.txt", self.categoricalColumnMappingFilePath)]

    def doColumnCheck(self):
        return False

    def getDebugArtifacts(self):
        return [{"categoricalmapping.json": self.categoricalColumnMapping}]

    def getOutputColumns(self):
        return [(create_column(k, "FLOAT"), [k]) for k, _ in self.categoricalColumnMapping.iteritems()]

    def getRTSMainModule(self):
        return "assignconversionrate"

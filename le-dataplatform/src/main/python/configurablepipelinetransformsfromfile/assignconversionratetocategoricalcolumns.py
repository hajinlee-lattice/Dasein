from pipelinefwk import get_logger
from pipelinefwk import create_column
from pipelinefwk import PipelineStep
'''
Description:
If the choice is made to assign a categorical variable to numeric values, the choice should be the conversion rate for that specitic categorical value.
For example, category A has 0% conversion, B has 1% conversion, and C has 8% conversion in the training set.  Reassign A,B, and C to 0,.01 and .08, respectively
'''
logger = get_logger("pipeline")

class AssignConversionRateToCategoricalColumns(PipelineStep):

    columnList = []
    categoricalColumnMapping = {}
    currentColumn = None
    totalPositiveThreshold = 12
    trainingMode = False

    def __init__(self, columnsToPivot, targetColumn, totalPositiveThreshold=12):
        self.totalPositiveThreshold = totalPositiveThreshold
        self.columnsToPivot = columnsToPivot
        self.targetColumn = targetColumn
        if columnsToPivot:
            self.columnList = columnsToPivot.keys()
        else:
            self.columnList = []
        logger.info("Initialized AssignConversionRate with columnsToPivot" + str(columnsToPivot) 
                    + ", targetColumn=" + str(targetColumn)
                    + ", positiveThreshold=" + str(self.totalPositiveThreshold))

    def transform(self, dataFrame, configMetadata, test):
        try:
            outputFrame = dataFrame
            for column, _ in self.columnsToPivot.iteritems():
                if column in dataFrame.columns:
                    self.currentColumn = column
                    outputFrame[column] = self.__assignConversionRateToCategoricalColumns(column, dataFrame, self.targetColumn)
                    
                    logger.info("self.assignConversionRateMapping " + str(self.categoricalColumnMapping))
            
            trainingMode = True
            return outputFrame
        except Exception:
            logger.exception("Caught Exception trying to use CleanCategoricalColumn Threshold Transformation")
            self.cleanCategoriesWithThreshold = None
            return dataFrame

    def __assignConversionRateToCategoricalColumns(self, column, dataFrame, tagetColumn):
        # Use the mapping learnt in training
        if self.trainingMode is False:
            if self.targetColumn in dataFrame and len(dataFrame[column]) == len(dataFrame[self.targetColumn]):
                logger.info("AssignConversionRate training phase. Converting column: " + column)
                allKeys = set(dataFrame[column])
                listOfValues=[x for x in enumerate(dataFrame[column])]
                totalPositiveEventCount=float(sum(dataFrame[self.targetColumn]))
                meanConversionRate=round(totalPositiveEventCount/(len(dataFrame[self.targetColumn])),2)
                keyConversionRate = {}
                for key in allKeys:
                    ind=[i for i,x in listOfValues if x==key]
                    count=len(ind)
                    if count > 0:
                        totalPositives=sum([dataFrame[self.targetColumn].iloc[i] for i in ind])
                        if totalPositives >= self.totalPositiveThreshold:
                            perc=round(sum([dataFrame[self.targetColumn].iloc[i] for i in ind])*1.0/count, 2)
                            keyConversionRate[key] = perc
                        else:
                            keyConversionRate[key] = meanConversionRate
                    else:
                        keyConversionRate[key] = 0.0
                self.categoricalColumnMapping[column] = keyConversionRate
                return map(self.applyConversionRate, dataFrame[column].values.tolist())
            else:
                return dataFrame[column]
        else:
            # Apply the conversion rates learnt during training
            logger.info("AssignConversionRate testing phase. Converting column: " + column)
            return map(self.applyConversionRate, dataFrame[column].values.tolist())

    def applyConversionRate(self, categoryValue):
        if self.currentColumn in self.categoricalColumnMapping:
            if categoryValue in self.categoricalColumnMapping[self.currentColumn]:
                if self.categoricalColumnMapping[self.currentColumn][categoryValue]:
                    return self.categoricalColumnMapping[self.currentColumn][categoryValue]
                else:
                    return categoryValue
            else:
                return categoryValue
        else:
            return categoryValue

    def getConversionKey(self):
        return self.categoricalColumnMapping

    def getOutputColumns(self):
        return [(create_column(k, "LONG"), [k]) for k in self.columnList]

    def getRTSMainModule(self):
        return "encoder"
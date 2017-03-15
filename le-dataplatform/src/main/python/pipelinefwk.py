import logging
import sys

import pandas as pd

try:
    from precisionutil import PrecisionUtil
except ImportError as e:
    from leframework.util.precisionutil import PrecisionUtil

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

def create_column(name, dataType):
    return { "name": name, "type": dataType }

logger = get_logger("pipelinefwk")

class Pipeline(object):
    pipelineSteps = []
    mediator = {}
    def __init__(self, pipelineSteps):
        self.pipelineSteps = pipelineSteps

    def getPipeline(self):
        return self.pipelineSteps

    def predict(self, dataFrame, configMetadata, test):
        transformed = dataFrame
        if len(dataFrame.index) > 0:
            transformed = dataFrame.apply(lambda x : PrecisionUtil.setPlatformStandardPrecision(x))
        for step in self.pipelineSteps:
            step.setMediator(self.mediator)
            try:
                transformed = step.transform(transformed, configMetadata, test)
                modifiedCols = [c[0]['name'] for c in step.getOutputColumns() if c[0]['name'] in transformed.columns.values]
                if len(modifiedCols) > 0:
                    transformed[modifiedCols] = transformed[modifiedCols].apply(lambda x : PrecisionUtil.setPlatformStandardPrecision(x))
            except Exception as e:
                logger.exception("Caught Exception while applying Transform. Stack trace below" + str(e))

        if 'ADDEDCOLUMNS' in self.mediator and self.mediator["ADDEDCOLUMNS"] is not None:
            logger.info('Columns added by Pipeline: {}'.format(str([c['ColumnName'] for c in self.mediator["ADDEDCOLUMNS"]])))
        else:
            logger.info('No columns added by Pipeline')
        if 'REMOVEDCOLUMNS' in self.mediator and self.mediator["REMOVEDCOLUMNS"] is not None:
            logger.info('Columns removed by Pipeline: {}'.format(str(self.mediator["REMOVEDCOLUMNS"])))
        else:
            logger.info('No columns removed by Pipeline')
        logger.info('Columns in DataFrame after Pipeline: {}'.format(str([c for c in transformed.columns.values])))

        return transformed

    def getMediator(self):
        return self.mediator

    '''
       This method only to be invoked from learning executor since it's learning from the data
       to determine how it can transform the data
    '''
    def learnParameters(self, trainingDataFrame, testDataFrame, configMetadata):
        for step in self.pipelineSteps:
            step.learnParameters(trainingDataFrame, testDataFrame, configMetadata)

class PipelineStep(object):
    modelStep = False
    props = {}

    def __init__(self, props):
        self.props = props
        
    def includeInScoringPipeline(self):
        return True

    def isModelStep(self):
        return self.modelStep

    def setModelStep(self, modelStep):
        self.modelStep = modelStep

    def learnParameters(self, trainingDataFrame, testDataFrame, configMetadata): pass

    def transform(self, dataFrame, configMetadata, test): pass

    def setProperty(self, propertyName, propertyValue):
        self.props[propertyName] = propertyValue

    def getProperty(self, propertyName):
        if propertyName in self.props:
            return self.props[propertyName]
        else:
            return None

    def getInputColumns(self):
        return []

    # Returns a list of a map to array of strings, where key is the output column name
    # and value is a list of column names as input
    def getOutputColumns(self):
        return []

    def getRTSMainModule(self):
        return None

    def getRTSArtifacts(self):
        return []

    def appendMetadataEntry(self, configMetadata, entry):
        mediator = self.getMediator()
        previousColumnsToAdd = self.getProperty("ADDEDCOLUMNS")
        if previousColumnsToAdd is None:
            previousColumnsToAdd = []
        previousColumnsToAdd.append(entry)
        self.setProperty("ADDEDCOLUMNS", previousColumnsToAdd)
        mediator["ADDEDCOLUMNS"] = previousColumnsToAdd
        

    def removeColumns(self, dataFrame, columnsToRemove):
        mediator = self.getMediator()
        for c in columnsToRemove:
            dataFrame.drop(c, axis=1, inplace=True)
        previousColumnsToRemove = self.getProperty("REMOVEDCOLUMNS")
        if previousColumnsToRemove is None:
            previousColumnsToRemove = []
        previousColumnsToRemove.extend(columnsToRemove)
        self.setProperty("REMOVEDCOLUMNS", previousColumnsToRemove)
        mediator["REMOVEDCOLUMNS"] = previousColumnsToRemove

    def doColumnCheck(self):
        return True

    def getDebugArtifacts(self):
        return []

    def getMediator(self):
        return self.getProperty("MEDIATOR")
    
    def setMediator(self, mediator):
        self.setProperty("MEDIATOR", mediator)
    
class ModelStep(PipelineStep):
    model = None
    modelInputColumns = []
    outputColumns = []
    scoreColumnName = ''

    def getModel(self):
        return self.model

    def getModelInputColumns(self):
        return self.modelInputColumns

    def __init__(self, model=None, modelInputColumns=None, scoreColumnName="Score"):
        self.model = model
        self.modelInputColumns = modelInputColumns
        self.scoreColumnName = scoreColumnName
        self.setModelStep(True)

    def clone(self, model, modelInputColumns, revenueColumnName, scoreColumnName="Score"):
        return ModelStep(model, modelInputColumns, scoreColumnName)

    def transform(self, dataFrame, configMetadata, test):
        ## From pandas release 0.17.0, .convert_objects is no longer available.
        ## .convert_objects(convert_numeric=True) had the behavior that, for a column "A",
        ## it would convert values to a numerical type if ANY of the values could be
        ## converted; values that could not be converted would be set to NaN.  If NONE
        ## of the values could be converted, the column would remain as-is.
        ## To reproduce this behavior with .to_numeric, the code below is needed.
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            try:
                dataFrame = dataFrame.convert_objects(convert_numeric=True)
            except:
                for col in dataFrame.columns:
                    convertedCol = pd.to_numeric(dataFrame[col], errors='coerce')
                    dataFrame[col] = convertedCol if not pd.isnull(convertedCol).all() else dataFrame[col]
        dataFrame.fillna(0, inplace=True)

        for columnName, columnData in dataFrame.iteritems():
            if columnData.dtype == object:
                dataFrame[columnName] = 0

        if "verbose" in self.model.__dict__:
            self.model.verbose = 0
        scoreColumn = self.model.predict_proba(dataFrame[self.modelInputColumns])
        # Check number of event classes
        index = 1 if len(scoreColumn[0]) == 2 else 0
        scoreColumn = scoreColumn[:, index]
        outputFrame = pd.DataFrame(scoreColumn, columns=[self.scoreColumnName])

        for columnName in self.modelInputColumns:
            outputFrame[columnName] = pd.Series(dataFrame[columnName].values, index=outputFrame.index)

        return outputFrame



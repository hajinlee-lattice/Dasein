import encoder
import numpy as np
import pandas as pd
from pipelinefwk import PipelineStep
from pipelinefwk import get_logger
import statsmodels.tsa.ar_model as am

logger = get_logger("evpipeline")

def get_predicted_revenue(row, colList):
    y = row[colList].as_matrix()
    y = y[y > 0]
    rev = 0
    
    try:
        mlag = len(y) / 2
        if mlag > 2:
            model = am.AR(y)
            results = model.fit(maxlag=mlag, ic='aic', trend='c', method='cmle')
            rev = results.predict().item(-1)
            if rev < 0.01:
                rev = 0
        elif len(y) == 0:
            rev = 0
        else:
            rev = np.mean(y)
    except:
        logger.info("No convergence, so using average")
        rev = np.mean(y)
        return rev
    return rev

class EnumeratedColumnTransformStep(PipelineStep):
    _enumMappings = {}

    def __init__(self, enumMappings):
        self._enumMappings = enumMappings
        
    def transform(self, dataFrame):
        outputFrame = dataFrame
        for column, encoder in self._enumMappings.iteritems():
            if hasattr(encoder, 'classes_'):
                classSet = set(encoder.classes_.flat)
                outputFrame[column] = outputFrame[column].map(lambda s: 'NULL' if s not in classSet else s)
                encoder.classes_ = np.append(encoder.classes_, 'NULL')
            
            if column in outputFrame:
                logger.info("Transforming column %s." % column)
                outputFrame[column] = encoder.transform(outputFrame[column])
     
        return outputFrame

class ColumnTypeConversionStep(PipelineStep):
    columnsToConvert_ = []
    
    def __init__(self, columnsToConvert=[]):
        self.columnsToConvert_ = columnsToConvert

    def transform(self, dataFrame):
        if len(self.columnsToConvert_) > 0:
            for column in self.columnsToConvert_:
                if column in dataFrame and dataFrame[column].dtype == np.object_:
                    dataFrame[column] = pd.Series(pd.lib.maybe_convert_numeric(dataFrame[column].as_matrix(), set(), coerce_numeric=True))
                else:
                    logger.warn("Column %s cannot be transformed since it is not in the data frame." % column)
        return dataFrame
    
class ImputationStep(PipelineStep):
    enumMappings = dict()
    targetCol = '' 
    
    def __init__(self, enumMappings, targetCol):
        self._enumMappings = enumMappings
        self.targetColumn = targetCol

    def transform(self, dataFrame):
        outputFrame = dataFrame
        if len(self._enumMappings) > 0:
            for column, value in self._enumMappings.iteritems():
                if column in outputFrame:
                    outputFrame[column] = outputFrame[column].fillna(value)
        return outputFrame
        
class EVModelStep(PipelineStep):
    
    def __init__(self, props):
        PipelineStep.__init__(self, props)
        self.setPostScoreStep(True)
        
    def getRequiredProperties(self):
        return ["provenanceProperties"]
        
    def transform(self, dataFrame):
        provenanceProps = self.props_["provenanceProperties"]
        colList = provenanceProps["EVModelColumns"].split(",")
        colList = [x for x in colList if x in dataFrame]
        
        outputFrame = pd.DataFrame(columns=["Score", "ExpectedRevenue"])
        outputFrame["Score"] = dataFrame["Score"]
        outputFrame["ExpectedRevenue"] = dataFrame.apply(lambda row: get_predicted_revenue(row, colList), axis=1)
        return outputFrame


from rulefwk import ColumnRule
from leframework.codestyle import overrides
from dataruleutilsDS import getRate
from dataruleutilsDS import getColVal
from dataruleutilsDS import getGroupedRate
from dataruleutilsDS import convertCleanDataFrame

import pandas as pd

class OverlyPredictiveDS(ColumnRule):

    def __init__(self, columns, categoricalColumns, numericalColumns, eventColumn, overlypredictiveLiftThreshold = 4, overlypredictivePopThreshold = 0.05, scale = 0.35):
        self.columns = columns.keys()
        self.catColumn = set(categoricalColumns.keys())
        self.numColumn = set(numericalColumns.keys())
        self.columnTypes = [self.getColumnType(col) for col in self.columns]
        self.overlypredictiveLiftThreshold = overlypredictiveLiftThreshold
        self.overlypredictivePopThreshold = overlypredictivePopThreshold
        self.scale = scale
        self.eventColName = eventColumn
        self.columnsInfo = {}
        self.groupedCountAndConversionRate = {}
        
    def getColumnType(self, column):
        if column in self.catColumn:
            return "cat"
        if column in self.numColumn:
            return "num"
        return None    
    
    def createInternalData(self, dataFrame):
        self.eventCol = [float(x) for x in dataFrame[self.eventColName].tolist()]
        self.data = convertCleanDataFrame(self.columns, dataFrame, self.catColumn, self.numColumn)
        
    @overrides(ColumnRule)
    def apply(self, dataFrame, dictOfArguments):
        self.createInternalData(dataFrame)       
        for columnName, colType in zip(self.columns, self.columnTypes):
            try:
                testResult = self.getOverlypredictive(columnName, colType, self.eventCol)
                self.columnsInfo[columnName] = testResult
            except KeyError:
                # What is default value
                self.columnsInfo[columnName] = None
        
    @overrides
    def getDescription(self):
        return "Check if column is overly positive predictive from a specific value or range"
    
    def getNumBucket(self, colVal):
        nullRate = len([1 for x in colVal if pd.isnull(x)]) * 1.0 /len(colVal) - 0.01
        p1 = self.overlypredictivePopThreshold * (1 - nullRate*self.scale)/(1-nullRate)
        return max(int(1/p1 + 0.01), 1)
       
    def getOverlypredictive(self, columnName, colType, eventCol):
    
        colVal = self.data[self.columns.index(columnName)] 
        
        cntRateOverall = (len(eventCol), getRate(eventCol))

        colValOrig = [x for x in colVal]        
        
        colVal = getColVal(colVal, colType, self.getNumBucket(colVal))
        
        cntRateGrouped = getGroupedRate(colVal, eventCol) 
        
        self.groupedCountAndConversionRate.update({columnName: cntRateGrouped})
        
        outputList = []
        
        for key, tup in cntRateGrouped.items():
            if colType == 'num':
                if key == 0:
                    bucketVal  = 'nan'
                else:
                    colValInBucket = [x1 for x1, x2 in zip(colValOrig, colVal) if x2 == key]
                    bucketVal  = (min(colValInBucket), max(colValInBucket))
            else:
                bucketVal = key
            
            popRate = tup[0]*1.0/cntRateOverall[0]
            lift = tup[1]*1.0/cntRateOverall[1]
                
            if colType == 'cat' and popRate >= self.overlypredictivePopThreshold and lift >= self.overlypredictiveLiftThreshold:
                outputList.append((True, bucketVal, popRate, lift))                
            elif colType == 'num' and lift >= self.overlypredictiveLiftThreshold:
                outputList.append((True, bucketVal, popRate, lift))
            else:
                outputList.append((False, bucketVal, popRate, lift))
        
        return outputList
        
    @overrides(ColumnRule)
    def getColumnsToRemove(self):
        return {key: any(y[0] for y in val)for key, val in self.columnsInfo.items()}

    def getSummaryPerColumn(self):
        return self.groupedCountAndConversionRate

    def getColumnsInfo(self):
        return self.columnsInfo
    

        
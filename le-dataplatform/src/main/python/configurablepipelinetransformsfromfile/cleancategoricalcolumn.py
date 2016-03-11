from collections import Counter
import math
import pandas as pd
from pipelinefwk import get_logger
from pipelinefwk import create_column
from pipelinefwk import PipelineStep
'''
Description:

This code ingests a categorical varaible list xlist and a threshhold value thresh
It then keeps only those categories which (from largest to smallest) comprise up to 95%
of the population.  It also puts a filter on that each category must
represent at least 1% of the toal population by the variable percMin

Running The Code Plus Notes: #note, test is run for eventList floats but in practice they will be integers
    aa=['0']*10+['1']*100+['2']*100+['3']*200+['4']*2+['5']*5+['7']*300+['8']*200+['9']*5
    elist=[.1]*10+[.1]*100+[.1]*100+[.1]*200+[.8]*2+[1.0]*5+[.12]*300+[.08]*200+[0]*5
    print elist
    bb=__cleanCateg(aa,.8)
    ff=__cleanCategFull(bb,aa,elist)
    z=[j for i, j in zip(bb, ff) if i != j]
'''

logger = get_logger("pipeline")

class CleanCategoricalColumn(PipelineStep):
    
    cleanCategoriesWithThreshold = None
    cleanCategoriesWithTargetColumn = None
    columnsToPivot = {}
    columnList = []
    nullValue = '0'
    includedKeys = {}
    targetColumn = ""
    currentColumn = None
    trainingMode = False

    def __init__(self, columnsToPivot, targetColumn):
        self.columnsToPivot = columnsToPivot
        self.targetColumn = targetColumn
        self.threshold = 0.95
        self.includedKeys = {}
        if columnsToPivot:
            self.columnList = columnsToPivot.keys()
        else:
            self.columnList = []

    def transform(self, dataFrame, configMetadata, test):
        threshold = self.getProperty("threshold")
        if self.threshold is None:
            threshold = 0.8
        targetColumn = self.getProperty("targetColumn")
        try:
            outputFrame = dataFrame
            if self.trainingMode is False:
                logger.info("CleanCategoricalColumn training phase.")
                for column, _ in self.columnsToPivot.iteritems():
                    if column in dataFrame.columns:
                        self.currentColumn = column
                        dataFrameAsList = dataFrame[column].values.tolist()
                        self.cleanCategoriesWithThreshold = self.__cleanCateg(dataFrameAsList, thresh=self.threshold)
                        outputFrame[column] = self.cleanCategoriesWithThreshold
                        self.cleanCategoriesWithTargetColumn = self.__cleanCategFull(column, dataFrame)
                        outputFrame[column] = self.cleanCategoriesWithTargetColumn
                self.trainingMode = True
            else:
                logger.info("CleanCategoricalColumn testing phase.")
                for column, _ in self.columnsToPivot.iteritems():
                    if column in dataFrame.columns:
                        self.currentColumn = column
                        dataFrameAsList = dataFrame[column].values.tolist()
                        outputFrame[column] = map(self.applyEmptyValue, dataFrame[column].values.tolist())

            return outputFrame
        except Exception:
            logger.exception("Caught Exception trying to use CleanCategolricalColumn Threshold Transformation")
            self.cleanCategoriesWithThreshold = None
            return dataFrame

    def applyEmptyValue(self, categoricalValue):
        if self.currentColumn in self.includedKeys:
            if categoricalValue in self.includedKeys[self.currentColumn]:
                return categoricalValue
            else:
                return self.nullValue
        else:
            return categoricalValue

    def __convertListToDataFrame(self, listToConvert):
        if type(listToConvert) is list:
            return pd.DataFrame(listToConvert)
        else:
            return listToConvert

    def __cleanCateg(self, xlist, thresh=.95, percMin=.01, nullValue='0'):
        self.nullValue = nullValue
        cc=Counter(xlist)
        total=len(xlist)*thresh
        cd=sorted(cc.items(), key = lambda x: x[1], reverse=True)
        newtotal=0
        includedKeys=[]
        threshCount=len(xlist)*percMin
        for cnt in range(len(cd)):
            xcount=cd[cnt][1]
            if xcount<threshCount: break
            newtotal += xcount
            includedKeys.append(cd[cnt][0])
            if newtotal>total*thresh: break
        cd = None
        cc = None
        self.includedKeys[self.currentColumn] = includedKeys

        return map(self.applyEmptyValue, xlist)
    '''
    This code revisits __cleanCateg results via eventList.
    cleanList is output of __cleanCateg
    The new list re-included those categories that are statistically signficant even though they are rare
    '''

    def __cleanCategFull(self, categoricalColumn, dataFrame):
        includedKeys=set(self.cleanCategoriesWithThreshold)
        excludedKeys=set(dataFrame[categoricalColumn])-set(self.cleanCategoriesWithThreshold)
        xlist = dataFrame[categoricalColumn]
        ylist=[x for x in enumerate(xlist)]
        popCount=len(xlist)
        popRate=(dataFrame[self.targetColumn].sum()*1.0) / popCount
        transferKeyList=[]
        for k in excludedKeys:
            ind=[i for i,x in ylist if x==k]
            count=len(ind)
            perc=round(sum([dataFrame[self.targetColumn].iloc[i] for i in ind])*1.0/count, 2)
            sd=math.sqrt(perc*(1.0-perc)/count + popRate*(1.0-popRate)/popCount)
            if abs(perc-popRate)>2.0*sd:
                transferKeyList.append(k)
        transferKeys=set(transferKeyList)
        includedKeys.update(transferKeys)
        self.includedKeys[self.currentColumn] = includedKeys
        return map(self.applyEmptyValue, xlist)

    def getIncludedKeys(self):
        return self.includedKeys

    def getOutputColumns(self):
        return [(create_column(k, "LONG"), [k]) for k in self.columnList]

    def getRTSMainModule(self):
        return "encoder"

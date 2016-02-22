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
    bb=cleanCateg(aa,.8)
    ff=cleanCategFull(bb,aa,elist)
    z=[j for i, j in zip(bb, ff) if i != j]
'''

logger = get_logger("pipeline")

class CleanCategoricalColumn(PipelineStep):
    
    cleanCategoriesWithThreshold = None
    cleanCategoriesWithTargetColumn = None
    columnsToConvert = {}
    columnList = []
    
    def __init__(self, columnsToConvert):
        self.columnsToConvert = columnsToConvert
        if columnsToConvert:
            self.columnList = columnsToConvert.keys()
        else:
            self.columnList = []
        pass

    def transform(self, dataFrame, threshold = 0.8, targetColumn=None):
        try:
            outputFrame = dataFrame
            for column, _ in self.columnsToConvert.iteritems():
                if column in dataFrame.columns:
                    dataFrameAsList = dataFrame[column].values.tolist()
                    self.cleanCategoriesWithThreshold = self.cleanCateg(dataFrameAsList,thresh=threshold)
                    outputFrame[column] = self.cleanCategoriesWithThreshold

            return outputFrame

        except Exception:
            logger.exception("Caught Exception trying to use CleanCategoricalColumn Threshold Transformation")
            self.cleanCategoriesWithThreshold = None
            return dataFrame

        if targetColumn is None:
            if self.cleanCategoriesWithThreshold is not None:
                return self.cleanCategoriesWithThreshold
            else:
                return dataFrame

        try:
            self.cleanCategoriesWithTargetColumn = self.cleanCategFull(self.cleanCategoriesWithThreshold,dataFrame, targetColumn)
            return self.cleanCategoriesWithTargetColumn
        except Exception:
            logger.exception("Caught Exception trying to use CleanCategoricalColumn TargetColumn Statistical Test Transformation")
            if self.cleanCategoriesWithThreshold:
                return self.cleanCategoriesWithThreshold
            else:
                return dataFrame

        return self.cleanCategoriesWithTargetColumn

    def convertListToDataFrame(self, listToConvert):
        if type(listToConvert) is list:
            return pd.DataFrame(listToConvert)
        else:
            return listToConvert

    def cleanCateg(self, xlist,thresh=.95,percMin=.01,nullValue='EMPTY'):
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
        for i in xrange(len(xlist)):
            if xlist[i] not in includedKeys:
                xlist[i] = nullValue
        return xlist

    '''
    This code revisits cleanCateg results via eventList.
    cleanList is output of cleanCateg
    The new list re-included those categories that are statistically signficant even though they are rare
    '''

    def cleanCategFull(self, cleanList,xlist,eventList,nullValue='EMPTY'):
        fullKeyList=set(xlist)
        includedKeys=set(cleanList)
        excludedKeys=set(xlist)-set(cleanList)
        ylist=[x for x in enumerate(xlist)]
        popCount=len(xlist)
        popRate=sum(eventList)*1.0/popCount
        transferKeyList=[]
        for k in excludedKeys:
            ind=[i for i,x in ylist if x==k]
            count=len(ind)
            perc=sum([eventList[i] for i in ind])*1.0/count
            sd=math.sqrt(perc*(1.0-perc)/count + popRate*(1.0-popRate)/popCount)
            if abs(perc-popRate)>2.0*sd:
                transferKeyList.append(k)
        transferKeys=set(transferKeyList)
        includedKeys.update(transferKeys)
        excludedKeys=fullKeyList-includedKeys
        newDict=dict((k,k) for k in includedKeys)
        newDict1=dict((k,nullValue) for k in excludedKeys)
        newDict.update(newDict1)
        return [newDict[x] for x in xlist]

    def getColumns(self):
        return self.columnList

    def getOutputColumns(self):
        return [(create_column(k, "LONG"), [k]) for k in self.columnList]

    def getRTSMainModule(self):
        return "encoder"

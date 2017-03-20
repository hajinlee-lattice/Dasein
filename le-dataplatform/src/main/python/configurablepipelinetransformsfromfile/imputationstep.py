from collections import OrderedDict
import json
from math import sqrt, fabs
import os

import pandas as pd
from pipelinefwk import PipelineStep
from pipelinefwk import create_column
from pipelinefwk import get_logger


logger = get_logger("pipeline")

class ImputationStep(PipelineStep):
    columns = OrderedDict()
    imputationValues = {}
    targetColumn = ""
    imputationFilePath = None

    def __init__(self, columns, imputationValues, targetCol):
        self.columns = columns
        self.imputationValues = imputationValues
        self.targetColumn = targetCol

    def getColumns(self):
        return self.columnList

    def getOutputColumns(self):
        return [(create_column(k, "FLOAT"), [k]) for k, _ in self.columns.iteritems()]

    def getRTSMainModule(self):
        return "replace_null_value_v2"

    def getRTSArtifacts(self):
        return [("imputations.json", self.imputationFilePath)]

    def getDebugArtifacts(self):
        return [{"imputationstep-imputationvalues.json": self.imputationValues}]

    def transform(self, dataFrame, configMetadata, test):
        if len(self.imputationValues) == 0:
            self.imputationValues = self.__computeImputationValues(dataFrame)
            self.__writeRTSArtifacts()
        for column, value in self.columns.iteritems():
            if column in dataFrame:
                try:
                    dataFrame[column] = dataFrame[column].fillna(self.imputationValues[column])
                except KeyError:
                    dataFrame[column] = dataFrame[column].fillna(value)
        return dataFrame

    def __writeRTSArtifacts(self):
        with open("imputations.json", "wb") as fp:
            json.dump(self.imputationValues, fp)
            self.imputationFilePath = os.path.abspath(fp.name)

    def __computeImputationValues(self, dataFrame):
        outputFrame = dataFrame
        imputationValues = {}
        eventVector = outputFrame[self.targetColumn].tolist()
        if len(self.columns) > 0:
            for column, value in self.columns.iteritems():
                if column in outputFrame:
                    try:
                        columnData = outputFrame[column].tolist()
                        imputationValues[column] = self.__imputeMissingNumValue(columnData, eventVector)
                    except KeyError:
                        imputationValues[column] = value
        return imputationValues

    # get list of ordered indices for x
    def __orderedIndicies(self, x, descending=False):
        return [y[0] for y in sorted(enumerate(x), key=lambda x: x[1], reverse=descending)]

    # this creates and populates mean values in each bin
    def __meanBin(self, a, idxbin):
        return sum(a[idxbin[0]:idxbin[1]]) * 1.0 / (idxbin[1] - idxbin[0])
        
    def __sgnFn(self, x):
        if x == 0:
            return x
        else:
            return (1 if x > 0 else (-1))
        
    # this returns the matched x value for the bin corresponding to the input y value
    def __matchValue(self, xList_notNull, eventList_notNull, binIndex, matchingRate):

        xListMean = [self.__meanBin(xList_notNull, (binIndex[i - 1], binIndex[i])) for i in range(1, len(binIndex))]
        eventListMean = [self.__meanBin(eventList_notNull, (binIndex[i - 1], binIndex[i])) for i in range(1, len(binIndex))]

        if len(xListMean) == 1:
            if eventListMean[0] > matchingRate:
                return min(xList_notNull) - 1
            else:
                return max(xList_notNull) + 1
        
        direction = self.__sgnFn(eventListMean[-1] - eventListMean[0])
        if min(eventListMean) > matchingRate:
            if direction == 1:
                return min(xList_notNull[binIndex[0]:binIndex[1]]) - 1
            else:
                return max(xList_notNull[binIndex[-2]:binIndex[-1]]) + 1
        elif max(eventListMean) < matchingRate:
            if direction == 1:
                return max(xList_notNull[binIndex[-2]:binIndex[-1]]) + 1
            else:
                return min(xList_notNull[binIndex[0]:binIndex[1]]) - 1
        else:
            matches = [fabs(matchingRate - p) for p in eventListMean]
            ind = [i for i, p in enumerate(matches) if p == min(matches)]
            return xListMean[ind[0]]

    def __getSig(self, sub, overall):
        sub_cnt, sub_rate = sub
        oa_cnt, oa_rate = overall
        r = (sub_cnt * sub_rate + oa_cnt * oa_rate) / (sub_cnt + oa_cnt)
        if r in [0, 1]:
            return 0
        else:
            return (sub_rate - oa_rate) / (sqrt(r * (1.0 - r) * (1.0 / sub_cnt + 1.0 / oa_cnt)))

    def __getTuple(self, eventCol):
        try:
            return (len(eventCol), sum(eventCol) * 1.0 / len(eventCol))
        except:
            return (len(eventCol), sum(float(x) for x in eventCol) / len(eventCol))
           
    def __createIndexSequenceByValue(self, xList, eventList, rawSplits=10):
        sp_0 = [0]
        xList_len = len(xList)
        posCurr = 0
        valCurr = xList[0]
        numPtsCurr = 0    
        divider = 3  

        stepSize = int(len(xList) / (rawSplits * divider))
        numPtsInBin = stepSize * divider
        
        if stepSize == 0:
            return [0, len(xList)]
            
        while posCurr < xList_len:
            posCurr = min(posCurr + stepSize, xList_len)
            if xList[posCurr - 1] == valCurr:
                numPtsCurr += stepSize
                continue
            elif xList[posCurr - 1] > valCurr:
                pos_temp = len([x for x in xList[posCurr - stepSize:posCurr] if x == valCurr])
                if numPtsCurr + pos_temp >= numPtsInBin:
                    sp_0.append(posCurr - stepSize + pos_temp)
                    numPtsCurr = stepSize - pos_temp
                    valCurr = xList[posCurr - stepSize + pos_temp]
                else:
                    numPtsCurr += stepSize
        sp_0.append(xList_len)
        
        sp_0_length = len(sp_0)
        bucket = 1
        while bucket < sp_0_length - 1:
            if xList[sp_0[bucket] - 1] == xList[sp_0[bucket]]:
                xListInBucket = xList[sp_0[bucket - 1]:sp_0[bucket]]
                sp_0[bucket] = sp_0[bucket] - len([x for x in xListInBucket if x == xList[sp_0[bucket]]])
            bucket = bucket + 1
        if sp_0[-1] < len(xList):
            sp_0.append(len(xList))

        return sp_0

        
    def __createIndexSequence(self, xList, eventList, rawSplits=10, sigThreshold=2):

        sp = self.__createIndexSequenceByValue(xList, eventList, rawSplits)

        bucket = 0
        while bucket < len(sp) - 2:
            eventList1 = eventList[sp[bucket]:sp[bucket + 1]]
            eventList2 = eventList[sp[bucket + 1]:sp[bucket + 2]]
            if fabs(self.__getSig(self.__getTuple(eventList1), self.__getTuple(eventList2))) < sigThreshold:
                del sp[bucket + 1]
            else:
                bucket += 1
        
        return tuple(sp)    

        
    '''
    input: 
        columnData: list of the numerical column
        eventVector: list of the events

    output:
       imputedValue: the imputed value that needs to be written to the Artifact for the scoring process, when there is no missing value, this output is None
    '''

    def __imputeMissingNumValue(self, columnData, eventVector, rawBinNumber=10, sigThreshold=2):
        
        nullInd = [pd.isnull(x) for x in columnData]
        nullLength = len([x for x in nullInd if x])
        
        xList_notmissing = [float(x[0]) for x in zip(columnData, nullInd) if not x[1]]
        if (len(columnData) - nullLength) / rawBinNumber < 20:
            rawBinNumber = int((len(columnData) - nullLength) / 20)
        
        if rawBinNumber <= 1:
            try:
                imputedValue = sum(xList_notmissing) * 1.0 / len(xList_notmissing)
            except ZeroDivisionError:
                imputedValue = 0.0
        else:
            idxOrdered = self.__orderedIndicies(xList_notmissing)
            
            xList_notmissing_ord = [xList_notmissing[i] for i in idxOrdered]
            
            eventList_notmissing = [x[0] for x in zip(eventVector, nullInd) if not x[1]]
            
            eventList_notmissing_ord = [eventList_notmissing[i] for i in idxOrdered]
                                    
            binIndex = self.__createIndexSequence(xList_notmissing_ord, eventList_notmissing_ord, rawBinNumber, sigThreshold)
            
            eventList_missing = [x[0] for x in zip(eventVector, nullInd) if x[1]]

            try:
                rate_null = sum(eventList_missing) * 1.0 / len(eventList_missing)
            except ZeroDivisionError:
                rate_null = sum(eventList_notmissing) * 1.0 / len(eventList_notmissing)

            imputedValue = self.__matchValue(xList_notmissing_ord, eventList_notmissing_ord, binIndex, rate_null)
            
        return imputedValue

import json
import os
import pandas as pd
from itertools import izip

from pipelinefwk import PipelineStep
from pipelinefwk import get_logger
from pipelinefwk import create_column
import random

from math import log
from collections import Counter

logger = get_logger("pipeline")

class TrfFunction(object):

    def execute(self, x):
        pass

class EmailLength(TrfFunction):

    def __init__(self, maxStrLen):
        self.maxStrLen = maxStrLen

    def execute(self, inputStr):
        try:
            if type(inputStr) == str:
                inputStr = inputStr.decode('utf-8','ignore')
            return float(min(len(inputStr), self.maxStrLen))
        except TypeError:
            return 0.0

class PrefixEmailLength(TrfFunction):
    def __init__(self, maxStrLen):
        self.maxStrLen = maxStrLen

    def execute(self, inputStr):
        try:
            if type(inputStr) == str:
                inputStr = inputStr.decode('utf-8','ignore')
            idx = inputStr.index('@')
            return float(min(idx, self.maxStrLen))
        except ValueError:
            return 0.0

class DomainLength(TrfFunction):
    def __init__(self, maxStrLen):
        self.maxStrLen = maxStrLen

    def execute(self, inputStr):
        try:
            if type(inputStr) == str:
                inputStr = inputStr.decode('utf-8','ignore')
            idx = inputStr.index('@')
            return float(min(len(inputStr)-idx-1, self.maxStrLen))
        except ValueError:
            return 0.0

class EmailIsInvalid(TrfFunction):

    def __init__(self, invalidEmailThld):
        self.invalidEmailThld = invalidEmailThld

    def execute(self, inputStr):
        if type(inputStr) == str:
            inputStr = inputStr.decode('utf-8','ignore')
        if len(inputStr) < self.invalidEmailThld:
            return True
        if '@' not in inputStr:
            return True
        else:
            return False


class AddEmailAttributesStep(PipelineStep):

    def __init__(self, params, addedFeatures, dsEmailImputation, targetCol, emailColName, maxStrLen, invalidEmailThld, thldCnt, thldPopPerc):

        self.features = []
        if params is not None and "features" in params["schema"] and params["schema"]["features"] != None:
            self.features = params["schema"]["features"]

        self.addedFeatures = addedFeatures

        self.dsEmailImputation = dsEmailImputation
        self.targetColumn = targetCol
        self.emailColumn = emailColName
        self.invalidEmailThld = invalidEmailThld
        self.maxStrLen = maxStrLen

        self.dsEmailImputationFilePath = None
        self.thldCnt = thldCnt
        self.thldPopPerc = thldPopPerc

        self.outputColumnsInput = {'DS_Email_IsInvalid': self.emailColumn, \
                                'DS_Email_Length': self.emailColumn ,\
                                'DS_Email_PrefixLength': self.emailColumn ,\
                                'DS_Domain_Length': self.emailColumn ,\
                                }

        self.origColumnNameList = [self.emailColumn]

        self.origColumnName = self.emailColumn

        self.columnsToRemove = set()

        logger.info('AddEmailAttributesStep: thldCnt={0}'.format(thldCnt))
        logger.info('AddEmailAttributesStep: thldPopPerc={0}'.format(thldPopPerc))
        logger.info('AddEmailAttributesStep: invalidEmailThld={0}'.format(invalidEmailThld))

        self.functionsToCall = {'DS_Email_IsInvalid': EmailIsInvalid(self.invalidEmailThld), \
                                'DS_Email_Length': EmailLength(self.maxStrLen),\
                                'DS_Email_PrefixLength': PrefixEmailLength(self.maxStrLen),\
                                'DS_Domain_Length': DomainLength(self.maxStrLen)}

        self.boolFeatures = set(['DS_Email_IsInvalid'])
        self.numFeatures = set(['DS_Email_Length','DS_Email_PrefixLength','DS_Domain_Length'])
        self.catFeatures = set([])

    def getRTSMainModule(self):
        return 'add_email_attributes'

    def getRTSArtifacts(self):
        return [("dsemailimputations.json", self.dsEmailImputationFilePath)]

    def getDebugArtifacts(self):
        return [{"dsemailfeaturesstep-dsemailimputationvalues.json": self.dsEmailImputation}]

    def getOutputColumns(self):
        return [(create_column(featureName, self.__getOutputColTypes(featureName)), [self.outputColumnsInput[featureName], featureName]) for featureName in self.addedFeatures]

    def doColumnCheck(self):
        return False

    def transform(self, dataFrame, configMetadata, test):

        if self.emailColumn not in dataFrame.columns.values:
            logger.info('Email is not found in the dataFrame')
            return dataFrame

        if self.emailColumn not in self.features:
            logger.info('Email is not a feature')
            return dataFrame

        if len(self.dsEmailImputation) != 0:
            logger.info('Email imputations already exist: {}'.format(str(self.dsEmailImputation)))

        featureValueDict = {}

        if not test:
            ## These parameters are needed to configure the RTS transformation
            self.dsEmailImputation['maxStrLen'] = self.maxStrLen
            self.dsEmailImputation['invalidEmailThld'] = self.invalidEmailThld

            colsInDataFrame = dataFrame.columns.values

            manyNullInd = {k: False for k in self.origColumnNameList}
            nullBooleanIndDict = {}
            for col in self.origColumnNameList:
                if col not in colsInDataFrame:
                    logger.info('{} is not found in the dataFrame'.format(col))
                    manyNullInd[col] = True
                else:
                    nullInd = [pd.isnull(x) for x in dataFrame[col]]
                    if sum(nullInd) >= len(nullInd)*(1-self.thldPopPerc) or sum(nullInd) >= len(nullInd) - self.thldCnt:
                        manyNullInd[col] = True
                        logger.info('{0} is null in {1} rows out of {2} rows in total'.format(col, str(sum(nullInd)), str(len(dataFrame.index))))
                    else:
                        nullBooleanIndDict[col] = nullInd

            if not manyNullInd[self.origColumnName]:
                self.addedFeatures = self.outputColumnsInput.keys()
                for featureName in self.addedFeatures:
                    nullBooleanIndDict[featureName] = nullBooleanIndDict[self.origColumnName]
                    if 'isnull' in featureName.lower():
                        featureValueDict[featureName] = nullBooleanIndDict[self.origColumnName]
                    else:
                        fnc = self.functionsToCall[featureName]
                        featureValueDict[featureName] = [fnc.execute(x) if not y else x for x,y in izip(dataFrame[self.origColumnName], nullBooleanIndDict[featureName])]

            if len(self.addedFeatures) > 0:
                eventList = dataFrame[self.targetColumn].tolist()

            for featureName, dsColVal in featureValueDict.iteritems():

                if 'isnull' in featureName.lower():
                    continue

                nullInd = nullBooleanIndDict[featureName]

                if featureName in self.numFeatures:
                    imputedValue, dsColVal = self.__fullValueMap(eventList, dsColVal, nullInd, True)
                else:
                    imputedValue, dsColVal = self.__fullValueMap(eventList, dsColVal, nullInd)

                if type(imputedValue) == float:
                    self.dsEmailImputation[featureName] = round(imputedValue, 2)
                    logger.info('column "{0}" has imputation value {1} (rounded to {2})'.format(featureName, imputedValue, self.dsEmailImputation[featureName]))
                else:
                    self.dsEmailImputation[featureName] = imputedValue
                    logger.info('column "{0}" has imputation value {1}'.format(featureName, imputedValue))

                # code that can be deleted in the future
                if featureName in self.catFeatures:
                    dd = self.__conversionRateEncoding(dsColVal, eventList)
                    dsColVal = [dd[x] for x in dsColVal]
                    self.dsEmailImputation[featureName] = [dd, imputedValue]
                else:
                    dsColVal = [float(x) for x in dsColVal]
                    imputedValue = float(imputedValue)
                    self.dsEmailImputation[featureName] = round(imputedValue, 2)

                featureValueDict.update({featureName: dsColVal})

            self.__appendMetadataEntryInBatches(configMetadata)
            self.__writeRTSArtifacts()
        else:
            if len(self.addedFeatures) > 0:

                origColValue = dataFrame[self.origColumnName].tolist()
                nullInd = [pd.isnull(x) for x in dataFrame[self.origColumnName]]

                for featureName in self.addedFeatures:

                    if '_isnull' in featureName.lower():
                        featureValueDict.update({featureName : nullInd})
                        continue

                    fnc = self.functionsToCall[featureName]
                    dsColVal = [fnc.execute(x) if not y else x for x,y in izip(origColValue, nullInd)]

                    if featureName in self.catFeatures:
                        try:
                            valmap =  self.dsEmailImputation[featureName][0]
                            imputedValue = valmap[self.dsEmailImputation[featureName][1]]
                            dsColVal = [valmap[x[0]] if x[0] in valmap else 0.0 if not x[1] else imputedValue for x in izip(dsColVal, nullInd)]
                        except KeyError:
                            logger.error('KeyError: check if {} is in dsEmailImputation'.format(featureName))
                            pass
                    else:
                        try:
                            dsColVal = [float(x[0]) if not x[1] else self.dsEmailImputation[featureName] for x in izip(dsColVal, nullInd)]
                        except KeyError:
                            logger.error('KeyError: check if {} is in dsEmailImputation'.format(featureName))
                            pass

                    featureValueDict.update({featureName : dsColVal})

        featureValueDict = {k: featureValueDict[k] for k in featureValueDict if k in self.addedFeatures}
        if len(featureValueDict) > 0:
            logger.info('Columns that are being added to the event table: {}'.format(str(featureValueDict.keys())))
            dataFrame = pd.concat([dataFrame, pd.DataFrame(featureValueDict, index = dataFrame.index.values)], axis = 1)

        colsToRemoveList = list(self.columnsToRemove)
        self.columnsToRemove = set([x for x in colsToRemoveList if x in dataFrame.columns.values])
        if len(self.columnsToRemove) > 0:
            self.removeColumns(dataFrame, self.columnsToRemove)
            logger.info('Columns that have been replaced by new Name Attributes: {}'.format(str(list(self.columnsToRemove))))

        return dataFrame




    def __conversionRateEncoding(self, columnList, eventList):
        def posrate(k):
            ind=[i for i,x in enumerate(columnList) if x==k]
            posEvents=sum([eventList[i] for i in ind])
            return round(posEvents*100.0/len(ind), 2)
        return dict((val,posrate(val)) for val in set(columnList))


    def __writeRTSArtifacts(self):
        with open("dsemailimputations.json", "wb") as fp:
            logger.info('Writing RTS artifacts: {}'.format(json.dumps(self.dsEmailImputation)))
            json.dump(self.dsEmailImputation, fp)
            self.dsEmailImputationFilePath = os.path.abspath(fp.name)

    def __appendMetadataEntryInBatches(self, configMetadata):
        for featureName in self.addedFeatures:
            statisticalType, fundamentalType, dataType = self.__getMetadataTypes(featureName)
            logger.info('Setting metadata for column {0}: StatisticalType={1}, FundamentalType={2}, DataType={3}'.format(featureName, statisticalType, fundamentalType, dataType))
            self.__appendMetadataEntry(configMetadata, featureName, statisticalType, fundamentalType, dataType)


    def __appendMetadataEntry(self, configMetadata, columnName, StatisticalType, FundamentalType, DataType):
        if configMetadata is None:
            return
        entry = {}
        entry["ColumnName"] = columnName
        entry["StatisticalType"] = StatisticalType
        entry["FundamentalType"] = FundamentalType
        entry["DataType"] = DataType
        super(AddEmailAttributesStep, self).appendMetadataEntry(configMetadata, entry)

    def __getOutputColTypes(self, featureName):
        return 'FLOAT'

    def __getMetadataTypes(self, featureName):
        return "ratio", "numeric", "Float"

    '''
    Assume a function f that operates on a customer provided field such as title and returns title length.
    We create an  imputed value to be used by the function and a new list of values
    The event table can be called by populating this new list of values and then calling the existing function

    Edge cases that we won't consider:
        null conv rate< sample conv rate so use this as an additional null indicator
        null rate is low enough that null can be replaced with single individual value

    Approach:
        separate out null values
        calculated imputed null value
        sample values randomly from non-nulls and replace non-nulls with these randomly sampled values
        return imputed non null value  (for RTS) and new 'corrected' list of values

    '''

    def __randomChoice(self, seq,numberReturned,seed=3):
        random.seed(seed)
        return [random.choice(seq) for i in xrange(numberReturned)]

    def __meanVal(self, x):
        if len(x)==0: return 0.0
        return sum(x)/float(len(x))


    def __nonContValueMapValue(self, nonNullEvents,nonNullValues,convRate):
        def eventMean(val):
            eventMapping=[nonNullEvents[i] for i,x in enumerate(nonNullValues) if x==val]
            return self.__meanVal(eventMapping)
        dictMap={x:abs(eventMean(x)-convRate) for x in set(nonNullValues)}
        return [x for x in dictMap.keys() if dictMap[x]==min(dictMap.values())][0]

    def __contValueMapValue(self, nonNullEvents,nonNullValues,convRate,numBuckets=20):
        ix=sorted(range(len(nonNullValues)), key = lambda i: nonNullValues[i],reverse=False)
        bucketLength=int(len(nonNullValues)/float(numBuckets)+.01)
        if bucketLength<1:
            bucketLength=1
            numBuckets=len(nonNullValues)
        def bucketRange(i):
            if i!=numBuckets-1:
                return [ix[k]  for k in  range(i*bucketLength,(i+1)*bucketLength)]
            else:
                return [ix[k] for k  in range(i*bucketLength,len(nonNullValues))]
        def bucketRate(i):
            bRange=[nonNullEvents[k] for k in bucketRange(i)]
            return self.__meanVal(bRange)
        bRates=[abs(bucketRate(i)-convRate) for i in range(numBuckets)]
        bucket=[i for i in range(numBuckets) if bRates[i]==min(bRates)][0]
        return self.__meanVal([nonNullValues[i] for i in bucketRange(bucket)])

    def __valueMapValue(self, nonNullEvents,nonNullValues,convRate,contValue=False,numBuckets=20):
        if contValue:
            return self.__contValueMapValue(nonNullEvents,nonNullValues,convRate,numBuckets)
        else:
            return self.__nonContValueMapValue(nonNullEvents,nonNullValues,convRate)


    def __fullValueMap(self, events, values, nullBooleanIndicator,contValue=False,numBuckets=20):
        sampleConvRate=self.__meanVal(events)
        ixNull=[i for i,x in enumerate(nullBooleanIndicator) if x] #assume x=True or False
        if len(ixNull)==0:
            return self.__valueMapValue(events,values,sampleConvRate,contValue,numBuckets), values
        nullConvRate = self.__meanVal([events[i] for i in ixNull])
        ixNonNull=[i for i in range(len(events)) if i not in  set(ixNull)]

        nonNullConvRate=self.__meanVal([events[i] for i in ixNonNull])

        nonNullEvents=[events[i] for i in ixNonNull]
        nonNullValues=[values[i] for i in ixNonNull]
        mappedNullValue=self.__valueMapValue(nonNullEvents,nonNullValues,nonNullConvRate,contValue,numBuckets)

        nullUseValues=self.__randomChoice(nonNullValues,len(ixNull))
        nonNullUseValues=[values[i] for i in ixNonNull]

        #these dictionaries  map
        iDictNull={j:i for i,j in enumerate(ixNull)}
        iDictNonNull={j:i for i,j in enumerate(ixNonNull)}
        def getVal(i):
            if nullBooleanIndicator[i]:
                return nullUseValues[iDictNull[i]]
            else:
                return nonNullUseValues[iDictNonNull[i]]
        return mappedNullValue, [getVal(i) for i in xrange(len(values))]

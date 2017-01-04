import json
import os
import pandas as pd
from itertools import izip

from pipelinefwk import PipelineStep
from pipelinefwk import get_logger
from pipelinefwk import create_column
import random

logger = get_logger("pipeline")


class TitleTrfFunction(object):

    def execute(self, x):
        pass

class GetStrLength(TitleTrfFunction):

    def __init__(self, maxTitleLen):
        self.maxTitleLen = maxTitleLen

    def execute(self, inputStr):
        if inputStr is None:
            return None
        try:
            ## It is possible that inputStr is actually a byte array, which in Python 2.7 is stored
            ## in a string type.  If this is the case, then we need to decode the byte array
            ## to a unicode object.
            if type(inputStr) == str:
                inputStr = inputStr.decode('utf-8')
            return float(min(len(inputStr), self.maxTitleLen))
        except TypeError:
            return 0.0

class AddTitleAttributesStep(PipelineStep):

    def __init__(self, dsTitleImputation, targetCol, titleColName, maxTitleLen, missingValues):
        self.dsTitleImputation = dsTitleImputation
        self.targetColumn = targetCol
        self.titleColumn = titleColName
        self.dsTitleImputationFilePath = None
        self.maxTitleLen = maxTitleLen
        self.missingValues = missingValues
        self.functionsToCall = {'DS_TitleLength': GetStrLength(maxTitleLen)}
        self.columnsToRemove = set(['Title_Length'])
        logger.info('AddTitleAttributesStep: maxTitleLen={0}; missingValues={1}'.format(maxTitleLen, str(missingValues)))

    def getRTSMainModule(self):
        return 'add_title_attributes'

    def getRTSArtifacts(self):
        return [("dstitleimputations.json", self.dsTitleImputationFilePath)]

    def getDebugArtifacts(self):
        return [{"dstitlefeaturesstep-dstitleimputationvalues.json": self.dsTitleImputation}]

    def getOutputColumns(self):
        return [(create_column(featureName, self.__getOutputColTypes(featureName)), [self.titleColumn, featureName]) for featureName, _ in self.functionsToCall.iteritems()]

    def doColumnCheck(self):
        return False

    def transform(self, dataFrame, configMetadata, test):

        if self.titleColumn not in dataFrame.columns.values:
            return dataFrame

        calculateImputationValues = True
        if len(self.dsTitleImputation) != 0:
            logger.info('Title imputations already exist: {}'.format(str(self.dsTitleImputation)))
            calculateImputationValues = False

        ## These parameters are needed to configure the RTS transformation
        self.dsTitleImputation['maxTitleLen'] = self.maxTitleLen
        self.dsTitleImputation['missingValues'] = self.missingValues

        titleList = dataFrame[self.titleColumn].tolist()

        titleFeatureDict = {}

        for featureName, function in self.functionsToCall.iteritems():

            dsColVal = [function.execute(x) for x in titleList]
            
            if featureName in ['DS_Title_HasUnusualChar','DS_Title_IsAcademic','DS_Title_IsTechRelated', \
                                 'DS_Title_IsDirector', 'DS_Title_IsSenior', 'DS_Title_IsVPAbove']:
                continue

            nullBooleanInd = [self.__ismissing(x) for x in dsColVal]

            if calculateImputationValues:
                self.dsTitleImputation[featureName] = 0.0
                eventList = dataFrame[self.targetColumn].tolist()
                if sum(nullBooleanInd) > 1 and  sum(nullBooleanInd) < len(nullBooleanInd):
                    if featureName == 'DS_TitleLength':
                        imputedValue, dsColVal = self.__fullValueMap(eventList, dsColVal, nullBooleanInd, True)
                    else:
                        imputedValue, dsColVal = self.__fullValueMap(eventList, dsColVal, nullBooleanInd)
                    self.dsTitleImputation[featureName] = round(imputedValue, 2)
                    logger.info('Title column "{0}" has imputation value {1} (rounded to {2})'.format(featureName, imputedValue, self.dsTitleImputation[featureName]))
            else:
                try:
                    dsColVal = [x[0] if not x[1] else self.dsTitleImputation[featureName] for x in izip(dsColVal, nullBooleanInd)]
                except KeyError:
                    logger.error('KeyError')
                    pass
            titleFeatureDict.update({featureName : dsColVal})

        if calculateImputationValues:
            self.__appendMetadataEntryInBatches(configMetadata)
            self.__writeRTSArtifacts()

        logger.info('Columns that are being added to the event table: {}'.format(str(titleFeatureDict.keys())))
        dataFrame = pd.concat([dataFrame, pd.DataFrame(titleFeatureDict, index = dataFrame.index.values)], axis = 1)

        logger.info('Columns that have been replaced by new Title Attributes: {}'.format(str(list(self.columnsToRemove))))
        self.removeColumns(dataFrame, self.columnsToRemove)

        return dataFrame

    def __writeRTSArtifacts(self):
        with open("dstitleimputations.json", "wb") as fp:
            logger.info('Writing RTS artifacts: {}'.format(json.dumps(self.dsTitleImputation)))
            json.dump(self.dsTitleImputation, fp)
            self.dsTitleImputationFilePath = os.path.abspath(fp.name)

    def __appendMetadataEntryInBatches(self, configMetadata):
        for featureName, _ in self.functionsToCall.iteritems():
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
        super(AddTitleAttributesStep, self).appendMetadataEntry(configMetadata, entry)

    def __ismissing(self, x):
        return pd.isnull(x) or (x in self.missingValues)

    def __getOutputColTypes(self, featureName):
        if featureName in ['DS_TitleLength', 'DS_Title_Level']:
            return 'FLOAT'
        elif featureName in ['DS_Title_Channel', 'DS_Title_Function', 'DS_Title_Level_Categorical', 'DS_Title_Role', 'DS_TItle_Scope']:
            return 'STRING'
        else:
            return 'BOOLEAN'

    def __getMetadataTypes(self, featName):
        if featName in ['DS_TitleLength', 'DS_Title_Level']:
            return "ratio", "numeric", "Float"
        elif featName in ['DS_Title_Channel', 'DS_Title_Function', 'DS_Title_Level_Categorical', 'DS_Title_Role', 'DS_TItle_Scope']:
            return "nominal", "alpha", "String"
        else:
            return "nominal", "boolean", "Bool"


    '''
    Assume a function f that operates on a customer provided field such as title and returns title length.
    We create an  imputed value to be used by the function and a new list of values
    The event table can be called by populating this new list of  values and then calling the existing function

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

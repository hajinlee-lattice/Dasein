import json
import os
import pandas as pd
from itertools import izip
import re

from pipelinefwk import PipelineStep
from pipelinefwk import get_logger
from pipelinefwk import create_column
import random

logger = get_logger("pipeline")

class TitleTrfFunction(object):

    def __init__(self, unusualCharacterSet, badSet):
        self.unusualCharacterSet = re.compile(unusualCharacterSet, re.IGNORECASE)
        self.badSet = re.compile(badSet, re.IGNORECASE)

    def execute(self, x):
        pass

    def valueReturn(self, x, mappingList):
        if x is None:
            return ''
        if len(x)==0:
            return ''

        for title, keys in mappingList:
            if keys.search(x) is not None:
                return title

        if self.dsHasUnusualChar(x):
            return 'invalid'

        return ''

    def isNumber(self, x):
        try:
            y = float(x)
            return True
        except:
            return False

    def dsHasUnusualChar(self, x):
        if self.isNumber(x):
            return True
        if self.unusualCharacterSet.search(x) is not None:
            return True
        if self.badSet.search(x) is not None:
            return True
        return False

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

class DSTitleMap(TitleTrfFunction):

    def __init__(self, unusualCharacterSet, badSet, mapTitle):
        super(DSTitleMap, self).__init__(unusualCharacterSet, badSet)
        self.mapTitle = [[title, re.compile(pattern, re.IGNORECASE)] for title, pattern in mapTitle]

    def execute(self, inputStr):
        return self.valueReturn(inputStr, self.mapTitle)

class DSTitleMapBool(TitleTrfFunction):

    def __init__(self, mapTitle):
        self.mapTitle = re.compile(mapTitle, re.IGNORECASE)

    def execute(self, inputStr):
        return self.mapTitle.search(inputStr) is not None

class DSTitleLevelCategorical(TitleTrfFunction):

    def __init__(self, unusualCharacterSet, badSet, mgrStr, dirStr, vpStr):
        super(DSTitleLevelCategorical, self).__init__(unusualCharacterSet, badSet)
        self.mgrStr = re.compile(mgrStr, re.IGNORECASE)
        self.dirStr = re.compile(dirStr, re.IGNORECASE)
        self.vpStr = re.compile(vpStr, re.IGNORECASE)

    def execute(self, inputStr):
        if self.vpStr.search(inputStr) is not None:  return 'VP'
        if self.dirStr.search(inputStr) is not None: return 'Director'
        if self.mgrStr.search(inputStr) is not None:  return 'Manager'
        if self.dsHasUnusualChar(inputStr): return 'invalid'
        return ''


class DSTitleHasUnusualChar(TitleTrfFunction):

    def execute(self, inputStr):
        return self.dsHasUnusualChar(inputStr)


class DSTitleLevel(TitleTrfFunction):

    def __init__(self, senrStr, mgrStr, dirStr, vpStr):
        self.senrStr = re.compile(senrStr, re.IGNORECASE)
        self.mgrStr = re.compile(mgrStr, re.IGNORECASE)
        self.dirStr = re.compile(dirStr, re.IGNORECASE)
        self.vpStr = re.compile(vpStr, re.IGNORECASE)

    def execute(self, inputStr):
        isVPAbove = (self.vpStr.search(inputStr) is not None)
        isDirector = (self.dirStr.search(inputStr) is not None)
        isManager = (self.mgrStr.search(inputStr) is not None)
        isSenior = (self.senrStr.search(inputStr) is not None)

        return float(isVPAbove*8+isDirector*4+isManager*2+isSenior)


class AddTitleAttributesStep(PipelineStep):

    def __init__(self, params, addedFeatures, dsTitleImputation, targetCol, titleColName, thldCnt, thldPopPerc, maxTitleLen):

        self.features = []
        if params is not None and "features" in params["schema"] and params["schema"]["features"] != None:
            self.features = params["schema"]["features"]

        self.addedFeatures = addedFeatures

        self.dsTitleImputation = dsTitleImputation
        self.targetColumn = targetCol
        self.titleColumn = titleColName
        self.thldCnt = thldCnt
        self.thldPopPerc = thldPopPerc
        self.dsTitleImputationFilePath = None
        self.maxTitleLen = maxTitleLen

        self.unusualCharacterSet = "[^\\w\\s]"

        self.badSet = "(_|\\b)(none|no|not|delete|asd|sdf|unknown|undisclosed|null|don|donot|abc|xyz|nonname|nocompany|noname)(_|\\b)"

        self.senrStr = "(_|\\b)(sr|senior)(_|\\b)"

        self.mgrStr = "(_|\\b)(mgr|mngr|manager)(.*)"

        self.dirStr = "(_|\\b)dir(ector)?(_|\\b)"

        self.vpStr = "(_|\\b)(vp|chief|c[tefmxdosi]o|pres|president|head[_ ]of|managing[_ ]partner)(_|\\b)"

        self.techStr = "(_|\\b)(eng|tech|info|programmer|developer)(.*)"

        self.acadStr = "(_|\\b)(student|researcher|professor)(_|\\b)"

        self.mapTitleChannelAny = [["Consumer", "(_|\\b)(consumer|retail)(_|\\b)"],
                            ["Government", "(_|\\b)(government)(_|\\b)"],
                            ["Corporate", "(_|\\b)(enterprise|corporate)(_|\\b)"]]

        self.mapTitleFunctionAny=[["IT","(_|\\b)(it|database|network|middleware|security)(_|\\b)|(_|\\b)info(.*)tech(.*)"],
                             ["Engineering","(_|\\b)(quality|system|engineer|developer?|software|testing|unix|linux|product)(_|\\b)"],
                             ["Finance","(_|\\b)(treasurer|tax|loan|risk)(_|\\b)|(_|\\b)(financ|purchas|account[ia])(.*)"],
                             ["Marketing","(_|\\b)(interactive|brand|mktg|content|social|event|media)(_|\\b)|(_|\\b)(market|generat|advertis|commerc)(.*)"],
                             ["Sales","(_|\\b)(sales?|channel|crm|field|inside)(_|\\b)|(_|\\b)accounts?(_|\\b)"],
                             ["Human Resource","(_|\\b)(hr|talent|benefits)(_|\\b)|(_|\\b)human(.*)resource(.*)"],
                             ["Operations","(_|\\b)(warehouse|plan|chain)(_|\\b)|(_|\\b)(operat|strateg|distribut|facilit)(.*)"],
                             ["Public Relations","(_|\\b)(communication|public|affairs|relation|community|pr)(_|\\b)"],
                             ["Design","(_|\\b)(creative|design|art|designer)(_|\\b)"],
                             ["Services","(_|\\b)(service|solution|training|implementation|user|help|care|maintenance|engage|instruction|account(.*)manager|account(.*)executive|soa)(_|\\b)"],
                             ["Research","(_|\\b)(research)(_|\\b)"],
                             ["Analytics","(_|\\b)data(_|\\b)|(_|\\b)analy(.*)"],
                             ["Academic","(_|\\b)education(_|\\b)|(_|\\b)academi(.*)"],
                             ["Consulting","(_|\\b)consult(.*)"]]

        self.mapTitleRoleAny=[["Associate", "(_|\\b)(assoc)(.*)"],
                      ["Assistant", "(_|\\b)(secret|assist)(.*)"],
                      ["Leadership", "(_|\\b)(founder|vice|vp|evp|chief|owner|president|svp|c[tefmxdosi]o)(_|\\b)"],
                      ["Manager", "(_|\\b)(mgr|gm|supervisor|lead)(_|\\b)|(_|\\b)(manager)(.*)"],
                      ["Director", "(_|\\b)dir(ector)?(_|\\b)"],
                      ["Engineer", "(_|\\b)(programmer|developer|dev|engineer)(_|\\b)"],
                      ["Consultant", "(_|\\b)(consultant)(_|\\b)"],
                      ["Student_Teacher", "(_|\\b)(instructor|coach|teacher|student|faculty)(_|\\b)"],
                      ["Analyst", "(_|\\b)(analyst)(_|\\b)"],
                      ["Admin", "(_|\\b)(admin|dba)(_|\\b)"],
                      ["Investor_Partner", "(_|\\b)(partner|investor|board)(_|\\b)"],
                      ["Controller_Accountant", "(_|\\b)(controller|accountant)(_|\\b)"],
                      ["Specialist_Technician", "(_|\\b)(technician|specialist)(_|\\b)"],
                      ["Architect", "(_|\\b)(architect)(_|\\b)"],
                      ["Representative", "(_|\\b)(representative)(_|\\b)"],
                      ["Editor", "(_|\\b)(editor)(_|\\b)"]]

        self.mapTitleScopeAny=[["Continental","(_|\\b)(north(.*)america|emea|asia|africa|europe|south(.*)america)(_|\\b)"],
                          ["Global","(_|\\b)(global|international|worldwide)(_|\\b)"],
                          ["National","(_|\\b)(us|national)(_|\\b)"],
                          ["Regional","(_|\\b)(region|branch|territory|district|central|western|eastern|northern|southern)(_|\\b)"]]
        self.functionsToCall = {'DS_Title_Length': GetStrLength(maxTitleLen), \
                                'DS_Title_Channel': DSTitleMap(self.unusualCharacterSet, self.badSet, self.mapTitleChannelAny), \
                                'DS_Title_Function': DSTitleMap(self.unusualCharacterSet, self.badSet, self.mapTitleFunctionAny), \
                                'DS_Title_Role': DSTitleMap(self.unusualCharacterSet, self.badSet, self.mapTitleRoleAny), \
                                'DS_Title_Scope': DSTitleMap(self.unusualCharacterSet, self.badSet, self.mapTitleScopeAny), \
                                'DS_Title_IsTechRelated':  DSTitleMapBool(self.techStr), \
                                'DS_Title_IsAcademic': DSTitleMapBool(self.acadStr), \
                                'DS_Title_IsVPAbove' : DSTitleMapBool(self.vpStr), \
                                'DS_Title_IsDirector' : DSTitleMapBool(self.dirStr), \
                                'DS_Title_IsManager' : DSTitleMapBool(self.mgrStr), \
                                'DS_Title_IsSenior' : DSTitleMapBool(self.senrStr), \
                                'DS_Title_Level_Categorical': DSTitleLevelCategorical(self.unusualCharacterSet, self.badSet, self.mgrStr, self.dirStr, self.vpStr), \
                                'DS_Title_HasUnusualChar': DSTitleHasUnusualChar(self.unusualCharacterSet, self.badSet), \
                                'DS_Title_Level':  DSTitleLevel(self.senrStr, self.mgrStr, self.dirStr, self.vpStr)
                                }
        self.columnsToRemove = set()
        self.boolFeatures = set(['DS_Title_HasUnusualChar','DS_Title_IsAcademic','DS_Title_IsTechRelated', 'DS_Title_IsDirector', 'DS_Title_IsSenior', 'DS_Title_IsVPAbove', 'DS_Title_IsManager'])
        self.numFeatures = set(['DS_Title_Length','DS_Title_Level'])
        self.catFeatures = set(['DS_Title_Channel','DS_Title_Function','DS_Title_Level_Categorical', 'DS_Title_Role', 'DS_Title_Scope'])

        logger.info('AddTitleAttributesStep: thldCnt={0}'.format(self.thldCnt))
        logger.info('AddTitleAttributesStep: thldPopPerc={0}'.format(self.thldPopPerc))
        logger.info('AddTitleAttributesStep: maxTitleLen={0}'.format(self.maxTitleLen))
        logger.info('AddTitleAttributesStep: unusualCharacterSet={0}'.format(self.unusualCharacterSet))
        logger.info('AddTitleAttributesStep: badSet={0}'.format(self.badSet))
        logger.info('AddTitleAttributesStep: mapTitleChannelAny={0}'.format(self.mapTitleChannelAny))
        logger.info('AddTitleAttributesStep: mapTitleFunctionAny={0}'.format(self.mapTitleFunctionAny))
        logger.info('AddTitleAttributesStep: mapTitleRoleAny={0}'.format(self.mapTitleRoleAny))
        logger.info('AddTitleAttributesStep: mapTitleScopeAny={0}'.format(self.mapTitleScopeAny))
        logger.info('AddTitleAttributesStep: techStr={0}'.format(self.techStr))
        logger.info('AddTitleAttributesStep: senrStr={0}'.format(self.senrStr))
        logger.info('AddTitleAttributesStep: mgrStr={0}'.format(self.mgrStr))
        logger.info('AddTitleAttributesStep: dirStr={0}'.format(self.dirStr))
        logger.info('AddTitleAttributesStep: vpStr={0}'.format(self.vpStr))
        logger.info('AddTitleAttributesStep: acadStr={0}'.format(self.acadStr))

    def getRTSMainModule(self):
        return 'add_title_attributes_v2'

    def getRTSArtifacts(self):
        return [("dstitleimputations.json", self.dsTitleImputationFilePath)]

    def getDebugArtifacts(self):
        return [{"dstitleattibutesstep-dstitleimputationvalues.json": self.dsTitleImputation}]

    def getOutputColumns(self):
        return [(create_column(featureName, self.__getOutputColTypes(featureName)), [self.titleColumn, featureName]) for featureName in self.addedFeatures]

    def doColumnCheck(self):
        return False

    def transform(self, dataFrame, configMetadata, test):

        if self.titleColumn not in dataFrame.columns.values:
            logger.info('Title is not found in the dataFrame')
            return dataFrame

        if self.titleColumn not in self.features:
            logger.info('Title is not a feature')
            return dataFrame

        if len(self.dsTitleImputation) != 0:
            logger.info('Title imputations already exist: {}'.format(str(self.dsTitleImputation)))

        nullBooleanInd = [pd.isnull(x) for x in dataFrame[self.titleColumn]]
        titleFeatureDict = {}

        if not test:
            ## These parameters are needed to configure the RTS transformation
            self.dsTitleImputation['maxTitleLen'] = self.maxTitleLen
            self.dsTitleImputation['unusualCharacterSet'] = self.unusualCharacterSet
            self.dsTitleImputation['badSet'] = self.badSet
            self.dsTitleImputation['mapTitleChannelAny'] = self.mapTitleChannelAny
            self.dsTitleImputation['mapTitleFunctionAny'] = self.mapTitleFunctionAny
            self.dsTitleImputation['mapTitleRoleAny'] = self.mapTitleRoleAny
            self.dsTitleImputation['mapTitleScopeAny'] = self.mapTitleScopeAny
            self.dsTitleImputation['techStr'] = self.techStr
            self.dsTitleImputation['senrStr'] = self.senrStr
            self.dsTitleImputation['mgrStr'] = self.mgrStr
            self.dsTitleImputation['dirStr'] = self.dirStr
            self.dsTitleImputation['vpStr'] = self.vpStr
            self.dsTitleImputation['acadStr'] = self.acadStr

            manyNullInd = False
            if sum(nullBooleanInd) >= len(nullBooleanInd)*(1-self.thldPopPerc) or sum(nullBooleanInd) >= len(nullBooleanInd) - self.thldCnt:
                logger.info('Title is null in {0} rows out of {1} rows in total'.format(str(sum(nullBooleanInd)), str(len(dataFrame.index))))
                manyNullInd = True

            if manyNullInd:
                self.addedFeatures = []
            else:
                self.addedFeatures = self.functionsToCall.keys()

                titleList = dataFrame[self.titleColumn].tolist()

                eventList = dataFrame[self.targetColumn].tolist()

                for featureName, function in self.functionsToCall.iteritems():
                    if function is None:
                        continue

                    dsColVal = [function.execute(x) if not y else x for x,y in izip(titleList, nullBooleanInd)]

                    if featureName in self.numFeatures:
                        imputedValue, dsColVal = self.__fullValueMap(eventList, dsColVal, nullBooleanInd, True)
                    else:
                        imputedValue, dsColVal = self.__fullValueMap(eventList, dsColVal, nullBooleanInd)

                    if type(imputedValue) == float:
                        self.dsTitleImputation[featureName] = round(imputedValue, 2)
                        logger.info('Title column "{0}" has imputation value {1} (rounded to {2})'.format(featureName, imputedValue, self.dsTitleImputation[featureName]))
                    else:
                        self.dsTitleImputation[featureName] = imputedValue
                        logger.info('Title column "{0}" has imputation value {1}'.format(featureName, imputedValue))

                    # code that can be deleted in the future
                    if featureName in self.catFeatures:
                        dd = self.__conversionRateEncoding(dsColVal, eventList)
                        dsColVal = [dd[x] for x in dsColVal]
                        self.dsTitleImputation[featureName] = [dd, imputedValue]
                    else:
                        dsColVal = [float(x) for x in dsColVal]
                        imputedValue = float(imputedValue)
                        self.dsTitleImputation[featureName] = round(imputedValue, 2)

                    titleFeatureDict.update({featureName : dsColVal})

            self.__appendMetadataEntryInBatches(configMetadata)
            self.__writeRTSArtifacts()

        else:
            if len(self.addedFeatures) > 0:

                titleList = dataFrame[self.titleColumn].tolist()

                for featureName, function in self.functionsToCall.iteritems():
                    if function is None:
                            continue

                    dsColVal = [function.execute(x) if not y else x for x,y in izip(titleList, nullBooleanInd)]

                    if featureName in self.catFeatures:
                        try:
                            valmap =  self.dsTitleImputation[featureName][0]
                            imputedValue = valmap[self.dsTitleImputation[featureName][1]]
                            dsColVal = [valmap[x[0]] if x[0] in valmap else 0.0 if not x[1] else imputedValue for x in izip(dsColVal, nullBooleanInd)]
                        except KeyError:
                            logger.error('KeyError: check if {} is in dsTitleImputation'.format(featureName))
                            pass
                    else:
                        try:
                            dsColVal = [float(x[0]) if not x[1] else self.dsTitleImputation[featureName] for x in izip(dsColVal, nullBooleanInd)]
                        except KeyError:
                            logger.error('KeyError: check if {} is in dsTitleImputation'.format(featureName))
                            pass
                    titleFeatureDict.update({featureName : dsColVal})

        titleFeatureDict = {k: titleFeatureDict[k] for k in titleFeatureDict if k in self.addedFeatures}
        if len(titleFeatureDict) > 0:
            logger.info('Columns that are being added to the event table: {}'.format(str(titleFeatureDict.keys())))
            dataFrame = pd.concat([dataFrame, pd.DataFrame(titleFeatureDict, index = dataFrame.index.values)], axis = 1)

        colsToRemoveList = list(self.columnsToRemove)
        self.columnsToRemove = set([x for x in colsToRemoveList if x in dataFrame.columns.values])
        if len(self.columnsToRemove) > 0:
            self.removeColumns(dataFrame, self.columnsToRemove)
        logger.info('Columns that have been replaced by new Title Attributes: {}'.format(str(list(self.columnsToRemove))))

        return dataFrame

    def __writeRTSArtifacts(self):
        with open("dstitleimputations.json", "wb") as fp:
            logger.info('Writing RTS artifacts: {}'.format(json.dumps(self.dsTitleImputation)))
            json.dump(self.dsTitleImputation, fp)
            self.dsTitleImputationFilePath = os.path.abspath(fp.name)

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
        super(AddTitleAttributesStep, self).appendMetadataEntry(configMetadata, entry)

    def __getOutputColTypes(self, featureName):
        return 'FLOAT'

    def __getMetadataTypes(self, featureName):
        return "ratio", "numeric", "Float"

    def __conversionRateEncoding(self, columnList, eventList):
        def posrate(k):
            ind=[i for i,x in enumerate(columnList) if x==k]
            posEvents=sum([eventList[i] for i in ind])
            return round(posEvents*100.0/len(ind), 2)
        return dict((val,posrate(val)) for val in set(columnList))

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

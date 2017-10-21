from collections import Counter
import json
from math import sqrt
import numbers
import os

import numpy as np
import pandas as pd
from pipelinefwk import PipelineStep
from pipelinefwk import create_column
from pipelinefwk import get_logger


logger = get_logger("categoricalgroupingstep")

class CategoricalGroupingStep(PipelineStep):

    def __init__(self, groupedFeatures, \
                 ldcFeatures, \
                 dsCategVarGroupingInfo, \
                 maxNumberUniqueValues, \
                 targetCol, \
                 categoricalColumns, \
                 params, \
                 thresholdError, \
                 thresholdSigSize, \
                 thresholdBinSize):
        self.ldcFeatures = set(ldcFeatures)
        self.groupedFeatures = groupedFeatures
        self.dsCategVarGroupingInfo = dsCategVarGroupingInfo
        self.maxNumberUniqueValues = maxNumberUniqueValues
        self.categoricalColumns = categoricalColumns

        self.keys = set()
        self.samples = set()
        self.readouts = set()
        self.target = set()

        if params is not None and 'schema' in params and params['schema'] is not None:
            if 'keys' in params['schema']:
                self.keys = params['schema']['keys']
            if 'samples' in params['schema']:
                self.samples = params['schema']['samples']
            if 'readouts' in params['schema']:
                self.readouts = params['schema']['readouts']
            if 'target' in params['schema']:
                self.target = params['schema']['target']

        self.targetColumn = targetCol

        self.dsCategVarGroupingInfoFilePath = None

        self.thresholdError = thresholdError
        self.thresholdSigSize = thresholdSigSize
        self.thresholdBinSize = thresholdBinSize

        self.columnsToRemove = set([])

        self.outputColumnsInput = {}

        self.featuresToExclude = {'Id', 'InternalId', 'Event', 'Domain', 'LastModifiedDate', 'CreatedDate', 'FirstName', 'LastName', 'Title', \
                    'Email', 'City', 'State', 'PostalCode', 'Country', 'PhoneNumber', 'Website', 'CompanyName', 'IsClosed', \
                    'StageName', 'AnnualRevenue', 'NumberOfEmployees', 'YearStarted'}

        logger.info('maxNumberUniqueValues={0}'.format(self.maxNumberUniqueValues))
        logger.info('thresholdError={0}'.format(self.thresholdError))
        logger.info('thresholdSigSize={0}'.format(self.thresholdSigSize))
        logger.info('thresholdBinSize={0}'.format(self.thresholdBinSize))

    def getRTSMainModule(self):
        return 'categorical_grouping'

    def getRTSArtifacts(self):
        return [("dscategvargroupinginfo.json", self.dsCategVarGroupingInfoFilePath)]

    def getDebugArtifacts(self):
        return [{"dscategvargroupingstep-dscategvargroupingstep.json": self.dsCategVarGroupingInfo}]

    def getOutputColumns(self):
        return [(create_column(featureName, self.__getOutputColTypes(featureName)), [self.outputColumnsInput[featureName]]) for featureName in self.groupedFeatures]

    def doColumnCheck(self):
        return False

    def transform(self, dataFrame, configMetadata, test):
        featureValueDict = {}

        if not test:

            eventList = dataFrame[self.targetColumn].tolist()
            popCount = len(eventList)
            popRate = sum(eventList) * 1.0 / len(eventList)

            featuresToGroup = self.__findFeatures(dataFrame, configMetadata)

            logger.info('features to group on = {}'.format(str(featuresToGroup)))

            for featureName in featuresToGroup:
                if featureName not in dataFrame.columns:
                    logger.info('feature not found in dataFrame = {}'.format(str(featureName)))
                    continue
                xList = dataFrame[featureName].tolist()
                if len(set(xList)) < self.maxNumberUniqueValues:
                    logger.info('feature {0} has {1} distinct values'.format(str(featureName), str(len(set(xList)))))
                    continue

                self.columnsToRemove.add(featureName)

                if any([pd.isnull(x) for x in xList]):
                    xList = ['LE-missing' if pd.isnull(x) else x for x in xList]

                featValMapping = self.__getGroupMapping(featureName, xList, eventList, popCount, popRate, self.thresholdError, self.thresholdSigSize, self.thresholdBinSize)

                if len(featValMapping) > 0 :
                    newFeatName = ''.join([featureName, 'Grouped'])
                    self.groupedFeatures.append(newFeatName)
                    self.outputColumnsInput.update({newFeatName: featureName})

                    xListNew = [featValMapping[x] for x in xList]

                    # code below that can be deleted in the future 
                    dd = self.__conversionRateEncoding(xListNew, eventList)
                    featValMapping = {self.__convertFloatToString(k):round(dd[v] / popRate, 2) for k, v in featValMapping.iteritems()}
                    xListNew = [round(dd[x] / popRate, 2) for x in xListNew]
                    # code above that can be deleted in the future

                    featureValueDict.update({newFeatName: xListNew})
                    self.dsCategVarGroupingInfo[featureName] = featValMapping

            self.__appendMetadataEntryInBatches(configMetadata)
            self.__writeRTSArtifacts()

        else:

            for newFeatName in self.groupedFeatures:

                origFeatName = newFeatName[:-7]

                logger.info('feature to be looked at in testing process {}'.format(origFeatName))

                if origFeatName not in dataFrame.columns:
                    logger.error('{} not in dataframe'.format(origFeatName))

                if origFeatName not in self.dsCategVarGroupingInfo:
                    logger.error('{} not found in dsCategVarGroupingInfo'.format(origFeatName))

                origColValue = dataFrame[origFeatName].apply(self.__convertFloatToString).tolist()
                if any([pd.isnull(x) for x in origColValue]):
                    origColValue = ['LE-missing' if pd.isnull(x) else x for x in origColValue]

                valmap = self.dsCategVarGroupingInfo[origFeatName]
                dsColVal = [valmap[x] if x in valmap else 1.0 for x in origColValue]

                featureValueDict.update({newFeatName : dsColVal})

        featureValueDict = {k: featureValueDict[k] for k in featureValueDict if k in self.groupedFeatures}
        if len(featureValueDict) > 0:
            logger.info('Columns that are being added to the event table: {}'.format(str(featureValueDict.keys())))
            dataFrame = pd.concat([dataFrame, pd.DataFrame(featureValueDict, index=dataFrame.index.values)], axis=1)

        colsToRemoveList = list(self.columnsToRemove)
        self.columnsToRemove = set([x for x in colsToRemoveList if x in dataFrame.columns.values])
        if len(self.columnsToRemove) > 0:
            self.removeColumns(dataFrame, self.columnsToRemove)
            logger.info('Columns that have been replaced by new Name Attributes: {}'.format(str(list(self.columnsToRemove))))

        return dataFrame

    def __convertFloatToString(self, x):
        return '{0:.2f}'.format(x) if (x is not None and isinstance(x, numbers.Real) and not np.isnan(x)) else x

    def __findFeatures(self, dataFrame, configMetadata):
        externalFeatures = super(CategoricalGroupingStep, self).getExternalColumnsSet(configMetadata)
        externalFeatures = filter(lambda x: x not in self.ldcFeatures, externalFeatures)
        featureList = [featureName for featureName in self.categoricalColumns if featureName in dataFrame.columns \
                and featureName not in self.keys and featureName not in self.samples and featureName not in self.readouts \
                and featureName not in self.target and featureName not in self.featuresToExclude]
        
        featureList = filter(lambda x: x not in externalFeatures, featureList)
        return featureList

    def __conversionRateEncoding(self, columnList, eventList):
        cCount = Counter(columnList)
        posCount = Counter([columnList[i] for i in range(len(columnList)) if eventList[i] == 1])
        yy = {k:(cCount[k], float(posCount[k])) for k in posCount.keys()}
        yy2 = {k:(cCount[k], 0.0) for k in set(cCount.keys()) - set(posCount.keys())}
        yy = dict(yy.items() + yy2.items())
        return {k:yy[k][1] / yy[k][0] for k in yy.keys()}

    def __writeRTSArtifacts(self):
        with open("dscategvargroupinginfo.json", "wb") as fp:
            logger.info('Writing RTS artifacts: {}'.format(json.dumps(self.dsCategVarGroupingInfo)))
            json.dump(self.dsCategVarGroupingInfo, fp)
            self.dsCategVarGroupingInfoFilePath = os.path.abspath(fp.name)

    def __appendMetadataEntryInBatches(self, configMetadata):
        for featureName in self.groupedFeatures:
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
        super(CategoricalGroupingStep, self).appendMetadataEntry(configMetadata, entry)

    def __getOutputColTypes(self, featureName):
        return 'FLOAT'

    def __getMetadataTypes(self, featureName):
        return "ratio", "numeric", "Float"

    def __getSig(self, sub, overall):
        """
        input: (sub-population size, sub-population rate), (overall-population size, overall-population rate)
        output: the significance difference between the subpopulation and the overall population
        """
        subCnt, subRate = sub
        oaCnt, oaRate = overall
        r = (subCnt * subRate + oaCnt * oaRate) / (subCnt + oaCnt)
        if r in [0, 1]:
            return 0
        else:
            return (subRate - oaRate) / (sqrt(r * (1.0 - r) * (1.0 / subCnt + 1.0 / oaCnt)))

    def __getTuple(self, xList, eventList, k):
        """
        input:  xList : feature column
                eventList: event column
                k : special value to look
        output: return (sub-population size, sub-population rate) with respect to the value of k
        """
        if pd.isnull(k):
            ind = [i for (i, x) in enumerate(xList) if pd.isnull(x)]
        else:
            ind = [i for (i, x) in enumerate(xList) if x == k]
        count = len(ind)
        if count == 0:
            rate = 0.0
        else:
            rate = sum([eventList[i] for i in ind]) * 1.0 / count    
        return (count, rate)    

    def __getGroupingResults(self, xList, eventList, popCount, popRate):
        """
        input:  xList : feature column
                eventList: event column
        output: (population size, conversion rate, significance) with respect to each distinct value from xList
        """
        fullValueSet = set(xList)
        output = {}
        for k in fullValueSet:
            count, rate = self.__getTuple(xList, eventList, k)
            sig = self.__getSig((count, rate), (popCount, popRate))
            output.update({k: (count, rate, sig)})
        return output

    def __groupCategoricalVar(self, xList, eventList, popCount, popRate, thresholdError=3, thresholdSigSize=0.01, threshBinSize=0.05):
        '''
        input:  xList : feature column
                eventList: event column
                thresholdError: threshold used to identify feature values that have significant conversion rate than the over all conversion rate
                thresholdBinSize: threshold used to identify the feature values that have big population size
                
        output: the set of feature values that:
            1. have significant conversion rate than the over all conversion rate
            2. have big population size
            3. have conversion rate close to the over all conversion rate
            4. don't belong to any of the 3 above sets
        '''
        groupingResults = self.__getGroupingResults(xList, eventList, popCount, popRate)

        featureValBigsize = dict([(x, y) for x, y in groupingResults.items() if y[0] >= popCount * threshBinSize])
        featureValSignificant = dict([(x, y) for x, y in groupingResults.items() if abs(y[2]) >= thresholdError * 0.05 * sqrt(popCount * popRate) and y[0] >= popCount * thresholdSigSize and x not in featureValBigsize.keys()])
        featureValClosetomean = dict([(x, y) for x, y in groupingResults.items() if abs(y[2]) <= thresholdError * 0.025 * sqrt(popCount * popRate) and x not in featureValBigsize.keys()])
        featureValLeftOver = dict([(x, y) for x, y in groupingResults.items() if x not in featureValBigsize.keys() and x not in featureValSignificant and x not in featureValClosetomean])

        return (featureValBigsize, featureValSignificant, featureValClosetomean, featureValLeftOver)

    def __getGroupMapping(self, featureName, xList, eventList, popCount, popRate, thresholdError=3, thresholdSigSize=0.01, threshBinSize=0.05):

        featValMapping = {}

        featureValBigsize, featureValSignificant, featureValClosetomean, featureValLeftOver = self.__groupCategoricalVar(xList, eventList, popCount, popRate, thresholdError, thresholdSigSize, threshBinSize)

        logger.info('featureValBigsize={}'.format(featureValBigsize))
        logger.info('featureValSignificant={}'.format(featureValSignificant))
        logger.info('featureValClosetomean={}'.format(featureValClosetomean))
        logger.info('featureValLeftOver={}'.format(featureValLeftOver))

        featValMapping.update({k:k for k in featureValSignificant.keys()})
        featValMapping.update({k:k for k in featureValBigsize.keys()})

        if len(featureValClosetomean) <= 1:
            featValMapping.update({k:k for k in featureValClosetomean.keys()})
        else:
            featValMapping.update({k: 'Close To Mean' for k in featureValClosetomean.keys()})

        if len(featureValLeftOver) > 0:

            values = featureValLeftOver.values()
            countLO = sum([x[0] for x in values])
            eventLO = sum([x[0] * x[1] for x in values])

            sigLO = self.__getSig((countLO, float(eventLO) / countLO), (popCount, popRate))

            logger.info('features in left_over aggregated: cnt={0}, event={1}, lift={2}, sig = {3}'.format(countLO, eventLO, eventLO / (countLO * popRate), sigLO))

            if sigLO <= thresholdError * 0.05 * sqrt(popCount * popRate):
                featValMapping.update({k: 'LeftOver' for k in featureValLeftOver.keys()})
            else:
                if len(featureValClosetomean) > 0:
                    values = featureValClosetomean.values()
                    countLOCM = countLO + sum([x[0] for x in values])
                    eventLOCM = eventLO + sum([x[0] * x[1] for x in values])
                    sigLOCM = self.__getSig((countLOCM, float(eventLOCM) / (countLOCM)), (popCount, popRate))
                    logger.info('features in left_over and close_to_mean when aggregated: cnt={0}, event={1}, lift={2}, sig = {3}'.format(countLOCM, eventLOCM, eventLOCM / (countLOCM * popRate), sigLOCM))

                    if sigLOCM <= thresholdError * 0.025 * sqrt(popCount * popRate):
                        featValMapping.update({k: 'Close To Mean' for k in featureValLeftOver.keys()})
                    else:
                        featValMapping = {}
                else:
                    featValMapping = {}

                if len(featValMapping) == 0:
                    logger.info('feature values in left_over are too significant; original column is to be deleted and not grouped, featureName={%s}' % featureName)

        return featValMapping

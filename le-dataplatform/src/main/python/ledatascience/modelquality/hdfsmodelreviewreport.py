# coding: utf-8
from __future__ import print_function

import csv
import fastavro as avro
import json
import logging
import math
import numpy as np
import os.path
import pandas as pd
import sys
import traceback
from collections import Counter
from itertools import chain
from itertools import groupby
from itertools import izip
from openpyxl.workbook import Workbook
from openpyxl.writer.excel import save_virtual_workbook

from HDFS_Params_ReviewReport import hdfsRevParam
from webhdfsagent import WebHDFSAgent

logger = logging.getLogger(name='hdfsmodelreviewreport')

class WrappedExcelFile(object):
    def __init__(self):
        self.wb = Workbook()
        self.sheetNumber = 1

    def printOutResults(self, data, cols, sheetName, set_width=False):
        print("Printing sheet number: " + str(self.sheetNumber) + " name: " + sheetName)
        if self.sheetNumber == 1:
            ws1 = self.wb.active
            ws1.title = sheetName
        else:
            ws1 = self.wb.create_sheet(title = sheetName)
        ws1.append(cols)
        for row in data:
            ws1.append(row)
        if set_width:
            dims = {}
            for row in ws1.rows:
                for cell in row:
                    if cell.value:
                        dims[cell.column] = max((dims.get(cell.column, 0), len(unicode(cell.value))))
            for col, value in dims.items():
                ws1.column_dimensions[col].width = value + 2
        self.sheetNumber = self.sheetNumber + 1

class ModelingEnvironment(object):

    instance = None

    @staticmethod
    def initialize(svr, port, user, id):
        ModelingEnvironment.instance = ModelingEnvironment(id,svr, port, user)

    @staticmethod
    def get_instance():
        return ModelingEnvironment.instance

    def __init__(self,runID, hdfsServer, hdfsPort, hdfsUser, tempDir='/tmp', deleteOnExit=True):
        self.localDir = tempDir + "/" + str(runID)
        self.deleteOnExit = deleteOnExit
        self.files = []
        self.hdfsAgent = WebHDFSAgent(hdfsServer, hdfsPort, hdfsUser)
        os.makedirs(self.localDir)

    def registerFile(self, fullyQualifiedFile):
        self.files.append(fullyQualifiedFile)

    def onShutdown(self):
        if(self.deleteOnExit):
            for file in self.files:
                os.remove(file)
            os.rmdir(self.localDir)

    # Entry point to remove markup such as "hdfs://serverName:port from file paths
    def normalizeSourceFileName(self, sourceFileName):
        return sourceFileName

    def getFileName(self, qualifiedFileName):
        parts = str(qualifiedFileName).split("/")
        return parts[-1]

    def getLocalFileName(self, sourceFileName):
        return os.path.join(self.localDir, self.getFileName(sourceFileName))

    def copy_local(self, sourceFileName):
        qualifiedTargetPath = self.getLocalFileName(sourceFileName)
        self.hdfsAgent.copyToLocal(sourceFileName, qualifiedTargetPath)
        self.registerFile(qualifiedTargetPath)
        return qualifiedTargetPath

    def load_csv_file(self, sourceFileName):
        try:
            qualifiedTargetPath = self.copy_local(sourceFileName)

            df = pd.read_csv(qualifiedTargetPath, engine='c', dtype=str)
            df = df.replace(np.nan, '', regex=True)
            cols = df.columns.tolist()
            rows = map(list, df.values)
            return (cols, rows)
        except:
            print("Failed to read CSV File: " + sourceFileName)
            raise Exception("Could not read file: " + sourceFileName)

    # Extract data needed from the json file
    def load_json_file(self, jFile):
        try:
            localFileName = self.copy_local(jFile)
            with open(localFileName, 'r') as fileHandle:
                jsondata = json.load(fileHandle)

            auc = jsondata['ModelDetails']['RocScore']
            liftData = jsondata['Segmentations'][0]['Segments']
            convRate = float(jsondata['ModelDetails']['TotalConversions'])/jsondata['ModelDetails']['TotalLeads']
            colList = [p['Name'] for p in jsondata['Predictors']]
            print("Successfully read json file: " + jFile)

            return auc, liftData, convRate,colList
        except:
            print("Could not read json file: " + jFile)
            raise Exception("Could not read file: " + jFile)

    def read_avro_file(self, avroFile):
        try:
            localFileName = self.copy_local(avroFile)
            with open(localFileName, 'rb') as f:
                reader = avro.reader(f)
                dictList = []
                for row in reader:
                    dictList.append(row)

            dictList = [dict([clean(k),clean(v)] for k,v in x.items()) for x in dictList]

            logger.info("Successfully read Avro File: " + str(avroFile))
            return dictList[0].keys(), [x.values() for x in dictList]

        except:
            logger.error("failed to read Avro file: " + avroFile)
            raise Exception("Could not read file" + avroFile)

    def writeDataToHDFS(self, targetFileName, sourceData):
        outputFileName = self.getLocalFileName(targetFileName)
        with open(outputFileName, "w") as localOutputFile:
            self.files.append(outputFileName)
            localOutputFile.write(sourceData)

        self.hdfsAgent.copyFromLocal(outputFileName, targetFileName, 1)


# Converts given list values to float
def ffloat(x):
    try:
        return float(x)
    except:
        return float('nan')

# from dictionary to list with many rows per key
def flatten_dict(ddict):
    outputList = [list(chain([key], i)) for key, item in ddict.iteritems() if len(item) >0 for i in item]
    return outputList

def discretizeNumericVar(inputCol, numBucket = 10, emptyValList=[None,"Null","Empty",""]):

    idxNonEmpty = [i for i,x in enumerate(inputCol) if x not in emptyValList and not math.isnan(x)]

    idxEmpty = [i for i,x in enumerate(inputCol) if x in emptyValList or math.isnan(x)]

    inputColNonEmpty=[inputCol[i] for i in idxNonEmpty]

    sortedInd = [y[0] for y in sorted(enumerate(inputColNonEmpty), key = lambda x: x[1])]

    idxNonEmpty_sorted = [idxNonEmpty[i] for i in sortedInd]

    idxCut = bucketing(len(idxNonEmpty), numBucket)

    return [idxEmpty] + [[idxNonEmpty_sorted[i] for i in range(idxCut[bucketIdx], idxCut[bucketIdx+1])] for bucketIdx in range(len(idxCut)-1)]

def numricVarToCategVar(inputCol, numBucket = 10, emptyValList=[None,"Null","Empty",""]):
    bucketIdxList = discretizeNumericVar(inputCol, numBucket)

    categIdx = []
    idxList = []
    for i in range(len(bucketIdxList)):
        if len(bucketIdxList[i]) > 0:
            idxList = idxList + bucketIdxList[i]
            categIdx = categIdx + [i]*len(bucketIdxList[i])

    sortedInd = [y[0] for y in sorted(enumerate(idxList), key = lambda x: x[1])]

    return [categIdx[i] for i in sortedInd]

# get event rate
def getRate(eventCol):
    try:
        return sum(eventCol)*1.0/len(eventCol)
    except:
        return sum(float(x) for x in eventCol)/len(eventCol)

# get the column of feature value. if it's a numerical column, discretize it into buckets
def getColVal(data, col_idx, colType, numBucket):
    if colType == 'cat':
        colVal = [x[col_idx] for x in data]
    elif colType == 'num':
        colVal = [ffloat(x[col_idx]) for x in data]
        colVal = numricVarToCategVar(colVal, numBucket)
    return colVal

def getGroupedRate(colVal, eventCol, cntRate_overall):

    cc = Counter(colVal)
    pp = {x: 0.0 for x in cc}
    pp.update(Counter([x for x,y in zip(colVal, eventCol) if y == 1]))

    # get count and count rate for each key in the above table. getRate gets the event rate for each group x[1] = event term in the colvalue, event tuple
    cntRate_grouped = {key : (cc[key], float(pp[key])/cc[key]) for key in cc}

    #x[0] = count for each category. x[1] = event rate for each category, sigcalculation is the significance calculation of conversion rate of each category in comparison to the overall conversion rate
    cntRate_grouped = {key : (x[0], x[1], sigCalculation(x, cntRate_overall)) for (key, x) in cntRate_grouped.items()}

    return cntRate_grouped

# given the length of data points and desirable number of buckets, return how many data points should be in each bucket
def bucketing(lenData, numBucket):
    numPtinBucket = int(lenData/float(numBucket))
    leftover = lenData - numPtinBucket*numBucket
    cutPosition=[(numPtinBucket+1)*i if i <= leftover else numPtinBucket*i+leftover for i in range(numBucket+1)]
    return cutPosition

# given the length and conversion rate of sub-population and the length and conversion rate of overall-population, calculate the significance
def sigCalculation(sub, overall):
    sub_cnt, sub_rate = sub
    oa_cnt, oa_rate = overall
    r = (sub_cnt*sub_rate + oa_cnt*oa_rate)/(sub_cnt+oa_cnt)
    return (sub_rate-oa_rate)/(math.sqrt(r*(1.0-r)*(1.0/sub_cnt + 1.0/oa_cnt)))

def clean(x):
    try:
        p=x.encode('ascii','ignore')
        return(p)
    except:
        return(x)

def readAvroFile(avroFile):
    reader = avro.reader(avroFile)
    dictList = []
    for row in reader:
        dictList.append(row)

    return dictList

# Load train and test data combined
def loadEventtable(train, test):

    #if os.path.isfile(train):
        #colNames_1, data_1 = ModelingEnvironment.get_instance().load_csv_file(train)
    #else:
        #train = ''.join([train[:-4],'.avro'])
        #test = ''.join([test[:-4],'.avro'])
        #colNames_1, data_1 = readInAvro(train)

    colNames_1, data_1 = ModelingEnvironment.get_instance().read_avro_file(train)

    if os.path.isfile(test):
        if '.csv' in test:
            colNames_2, data_2 =  ModelingEnvironment.get_instance().load_csv_file(test)
        else:
            colNames_2, data_2 =  ModelingEnvironment.get_instance().read_avro_file(test)

        len_1 = len([x for x in colNames_1 if colNames_1.index(x) == colNames_2.index(x)])
        if len_1 == len(colNames_2):
            data = data_1+data_2
            colNames = colNames_1
        else:
            print('Training and test data format are different')
            #return []
        del data_1,data_2
    else:
        data = [x for x in data_1]
        colNames = colNames_1
        del data_1

    return data, colNames

def findDomain(emailStr):
    if pd.isnull(emailStr):
        return ''
    try:
        position = emailStr.index('@')+1
        if emailStr[position:] is None:
            print('***{}***'.format(emailStr))
        return emailStr[position:]
    except ValueError:
        return ''

def is_a_number(a):
    try:
        float(a)
        return True
    except (ValueError, TypeError):
        return False

# given a column of data, decide if this is categorical or numerical
def getColumnType(columnData):
    def is_number(s):
        return map(lambda x: 1 if is_a_number(x) else 0, s)

    def is_notempty(s):
        return map(lambda x: 0 if x =='' else 1, s)

    def is_binary(s):
        return map(lambda x: 1 if x in ['0', '1'] else 0,s)

    def getColType(s):
        if sum(is_notempty(s)) != 0:
            if float(sum(is_binary(s)))/sum(is_notempty(s)) > 0.98:
                return 'cat'
            elif float(sum(is_number(s)))/sum(is_notempty(s)) > 0.98:
                return 'num'
            else:
                return 'cat'
        else:
            return 'cat'
    return getColType(columnData)

def getColTypes(params, data, colNames, colNameList, rf_data):
    try:
        qualifiedName = ModelingEnvironment.get_instance().copy_local(params.varType_file)
        with open(qualifiedName, "r") as varFileHandle:
            f_csv = csv.reader(varFileHandle)
            varType = [x for x in f_csv]

    except:
        print("Could not read var file handle: " + params.varType_file)
        raise Exception("Could not read var file: " + params.varType_file)

    varType = dict(varType)

    rfFeatures = [x[0] for x in rf_data]

    def getColNameInET(strg):
        if strg in colNames:
            return strg
        else:
            a = [xx for xx in colNames if strg.startswith(xx)]
            if len(a) == 0:
                return None
            return a[0]

    colNameList = [getColNameInET(x) for x in rfFeatures]
    colNameList = list(set(colNameList))

    colTypeList =  [varType[x] if x in varType else getColumnType([y[colNames.index(x)]for y in data]) for x in colNameList if x is not None]

    return (colNameList, colTypeList)

# Standard deviation is calculated for a given list of numbers
def calcStdDev(data):

    mean = sum(data)*1/len(data)

    variance = float(sum([((i-mean) * (i-mean)) for i in data])*1/len(data))

    return math.sqrt(variance)

# Source data match rate
# input: tp_data - top predictor file, parameters for min match rate values
# returns sources that do not match at the rate expected
def matchRate(param,tp_data, evtData, evtCol):

    null_Pop = {}
    null_Popchk = {}

    for i in param.source_cols.keys():
        #get source column names
        null_Pop[i] = [(x[0],ffloat(x[10]),ffloat(x[7])) for x in tp_data if i in x[0] and x[5] in param.no_data]
    # Source data match rate check only for the columns that are present in the top predictor file
    # BusinessEmployeesRange is not present in the sample usedS

    null_Popchk = {k: max([w[1] for w in v]) for k,v in null_Pop.items() if len(v) > 0}

    match_rate = {k: 1.0 for k in param.source_tp.values()}
    match_rate.update({param.source_tp[k]: 1-v for k,v in null_Popchk.items()})

    pubDomainInd = [str(x[evtCol.index(param.publicDomain)]) for x in evtData]
    pubDomainInd = [0 if x.lower() in ['false','0','0.0'] else 1 for x in pubDomainInd]

    #print sum(pubDomainInd), len(pubDomainInd)

    if sum(pubDomainInd) < 0.1*len(pubDomainInd):
        return match_rate, None

    matchPr_rate = {}
    for c in param.source_et.keys():
        colVal = [x[evtCol.index(c)] for x in evtData]
        matchPr_rate[param.source_et[c]] = 1-sum([1 if pd.isnull(x) or x == '' else 0 for x,y in zip(colVal, pubDomainInd) if not y])*1.0/(len(pubDomainInd) - sum(pubDomainInd))

    return match_rate, matchPr_rate


# Check if the auc is within the limits < 0.875 and > 0.70
# input: AUROC (auc), upper and lower limit for Lift
def aucCheck(auc, lower_lim = 0.7, upper_lim = 0.875):

    if auc < lower_lim or auc > upper_lim:
        return[('AUC check','AUC','Failed',auc)]
    else:
        return[('AUC check','AUC','Passed',auc)]

# check cols where Null pop >= 95%
# check columns with > 5% NULLs and lift > 1.05 lift for NULLs for column removal
# check cols with < 5% Null and Lift > 1.05 for rows removal
# input: tp_data = top predictor data, param = parameters
# output: list with issue type, column name and flag
def nullFlags(param,tp_data, nullLiftlimit = 1.02, nullPopLimit = 0.05):

    issues = []

    # check cols with < 95% Null and Lift > 1.05 for rows removal
    nullLiftissue = [('Null Lift - Remove', x[0],'Failed',ffloat(x[10]),float(x[7])) for x in tp_data if x[5] in param.no_data and float(x[10]) < (1-nullPopLimit) and float(x[7]) > nullLiftlimit]

    if nullLiftissue:
        issues.extend(nullLiftissue)

        # check cols with > 95% Null and Lift >1.02
    highNull_cols = [('High Null Columns - check feature', x[0],'Failed',ffloat(x[10]),float(x[7])) for x in tp_data if x[5] in param.no_data and float(x[10]) > (1-nullPopLimit) and float(x[7]) > nullLiftlimit]

    if highNull_cols:
        issues.extend(highNull_cols)

    return issues


# Function checks if public domain population is greater than 50%
# Input: top predictor data, public domain column name, threshold for public domain percent
def pubDomainpop(trData, trCols, eventCol, pubDomainId):

    pubDomainCol = [x[trCols.index(pubDomainId)] for x in trData]

    eventPubCol = [x[0] for x in zip(eventCol, pubDomainCol) if str(x[1]).lower() in ['true','1']]

    if len(eventPubCol) == 0:
        return None

    return len(eventPubCol)*1.0/len(pubDomainCol), sum(eventPubCol)*len(eventCol)*1.0/(len(eventPubCol)*sum(eventCol))

def scoreDistribution(cols, data, scoreColName, emailColname, domain=None):

    scoreCol = [float(x[cols.index(scoreColName)]) for x in data]

    domainCol = ['' if pd.isnull(x[evtCol.index(emailColname)]) else x[evtCol.index(emailColname)].lower() for x in evtData]

    emailFormat = ['@' in x for x in domainCol]

    if sum(emailFormat) > 0.5*len(emailFormat):
        domainCol = [findDomain(x) for x in domainCol]

    def getDist(x):
        x.sort()
        return [x[0], x[-1], sum(x)/len(x), x[len(x)/2]]
    if domain is None:
        domainSet = set(domainCol)
        return {name: getDist([s[1] for s in zip(domainCol, scoreCol) if s[0] == name]) for name in set(domainCol)}
    else:
        return getDist([s[1] for s in zip(domainCol, scoreCol) if s[0] == domain])


# Checks for non public domains, if number of leads > no. of domains
# Checks if the top count domains are from US based on top level domain extension
# Input full event table, topLeveldomain list
def emailDomaincheck(evtData, evtCol, pubColName, emailColname, eventCol):

    pubDomainInd = [str(x[evtCol.index(pubColName)]).lower() for x in evtData]

    domainColOrig = ['' if pd.isnull(x[evtCol.index(emailColname)]) else x[evtCol.index(emailColname)].lower() for x in evtData]

    emailFormat = ['@' in x for x in domainColOrig]

    if sum(emailFormat) > 0.5*len(emailFormat):
        domainColOrig = [findDomain(x) for x in domainColOrig]

    domainCol = [x for x, y in izip(domainColOrig,pubDomainInd) if y in ['false','0','0.0'] and x != '']

    # get the count of domains
    orderedCnt = Counter(domainCol).most_common()

    duplicateDomains = set([x[0] for x in orderedCnt if x[1] > 1])

    if len(duplicateDomains) == 0:
        return (None, None, None)

    eventCol = [x for x, y, z in izip(eventCol, pubDomainInd, domainColOrig) if y in ['false','0','0.0'] and z != '']

    duplicateDomainsEvent = [x for x,y in zip(eventCol, domainCol) if y in duplicateDomains]

    return (orderedCnt[:5], len(duplicateDomainsEvent)*1.0/len(domainCol), sum(duplicateDomainsEvent)*len(eventCol)*1.0/(len(duplicateDomainsEvent)*sum(eventCol)))


def dedupCheck(evtData, evtCol, pubColName, emailColname, lecountryColname, countryColname, eventCol):

    pubDomainInd = [str(x[evtCol.index(pubColName)]).lower() for x in evtData]

    if emailColname in evtCol:
        domainColOrig = ['' if pd.isnull(x[evtCol.index(emailColname)]) else x[evtCol.index(emailColname)].lower() for x in evtData]
    else:
        domainColOrig = []

    if lecountryColname in evtCol:
        lecountryCol = ['' if pd.isnull(x[evtCol.index(lecountryColname)]) else x[evtCol.index(lecountryColname)].lower() for x in evtData]
    else:
        lecountryCol = []

    if countryColname in evtCol:
        countryCol = ['' if pd.isnull(x[evtCol.index(countryColname)]) else x[evtCol.index(countryColname)].lower() for x in evtData]
        countryColorig = [x if x != '' else y for x, y in zip(lecountryCol, countryCol)]
    else:
        countryColorig = lecountryCol

    emailFormat = ['@' in x for x in domainColOrig]

    if sum(emailFormat) > 0.5*len(emailFormat):
        domainColOrig = [findDomain(x) for x in domainColOrig]

    domaincountryCol = [(x,y) for x, y, z in izip(domainColOrig, countryColorig, pubDomainInd) if z in ['false','0','0.0'] and x != '' and y != '']

    orderedCnt = Counter(domaincountryCol).most_common()

    dupDomainCntrys = set([x[0] for x in orderedCnt if x[1] > 1])

    if len(dupDomainCntrys) == 0:
        return (None, None, None)

    eventCol = [x for x, y, z1, z2 in izip(eventCol, pubDomainInd, domainColOrig, countryColorig) if y in ['false','0','0.0'] and z1 != '' and z2 != '']

    dupDomainCntryEvent = [x for x,y in zip(eventCol, domaincountryCol) if y in dupDomainCntrys]

    return (orderedCnt[:5], len(dupDomainCntryEvent)*1.0/len(domaincountryCol), sum(dupDomainCntryEvent)*len(eventCol)*1.0/(len(dupDomainCntryEvent)*sum(eventCol)))

def findTopLvlDomain(str):
    idx = str.rfind('.')
    if idx == -1:
        return ''
    return str[idx:]

def toplvlDomaincheck(evtData, evtCol, pubColName, emailColname, eventCol):

    pubDomainInd = [str(x[evtCol.index(pubColName)]).lower() for x in evtData]

    domainColOrig = ['' if pd.isnull(x[evtCol.index(emailColname)]) else x[evtCol.index(emailColname)].lower() for x in evtData]

    toplvlDomain = [findTopLvlDomain(x) for x,y in zip(domainColOrig, pubDomainInd) if y in ['false','0','0.0'] and x != '']

    # get the count of domains
    orderedCnt = Counter(toplvlDomain).most_common(4)

    eventCol = [x for x, y, z in izip(eventCol, pubDomainInd, domainColOrig) if y in ['false','0','0.0'] and z != '']

    if len(eventCol) == 0:
        return None
    else:
        rate = sum(eventCol)*1.0/len(eventCol)

    output = {d: (c*1.0/len(toplvlDomain), sum(x for x,y in izip(eventCol,toplvlDomain) if y == d)*1.0/(c*rate)) for d,c in orderedCnt}

    return output

# Check if a company name has more than one lead assigned
# Input: train+test event table and the event table columns
def leadsperCompany(evtData, evtCol,companyColumn, eventCol):

    companyColOrig = ['' if pd.isnull(x[evtCol.index(companyColumn)]) else x[evtCol.index(companyColumn)].lower() for x in evtData]

    companyCol = [x for x in companyColOrig if x != '']

    # get the count of domains
    orderedCnt = Counter(companyCol).most_common()

    duplicates = set([x[0] for x in orderedCnt if x[1] > 1])

    if len(duplicates) == 0:
        return (None, None, None)

    eventCol = [x for x, z in izip(eventCol, companyColOrig) if z != '']

    duplicatesEvent = [x for x,y in zip(eventCol, companyCol) if y in duplicates]

    return (orderedCnt[:5], len(duplicatesEvent)*1.0/len(companyCol), sum(duplicatesEvent)*len(eventCol)*1.0/(len(duplicatesEvent)*sum(eventCol)))


# checks the highly positively predictive values single variable analysis of individual features
def get_highly_positively_predictive_small_population(cntRate_grouped, cntRate_overall, colType, params):
    highly_positively_predictive_small_population = []
    # for categorical variable, all possible feature values need to be looked at
    if colType == 'cat':
        for key, x in cntRate_grouped.items():
            if x[0]*1.0/cntRate_overall[0] <= params.highly_positively_predictive_small_population_popPerc_threshold and x[1]*1.0/cntRate_overall[1] >= params.highly_positively_predictive_small_population_lift_threshold:
                highly_positively_predictive_small_population.append([colType, key, x[0], x[1], x[0]*x[1], x[1]/cntRate_overall[1], x[2]])
    # for numerical variable, only null value need to be looked at. rows with null values are marked at bucket 0
    elif colType == 'num':
        if 0 in cntRate_grouped.keys() and cntRate_grouped[0][0]*1.0/cntRate_overall[0] <= params.highly_positively_predictive_small_population_popPerc_threshold and cntRate_grouped[0][1]*1.0/cntRate_overall[1] >= params.highly_positively_predictive_small_population_lift_threshold:
            return [[colType, 'nan', cntRate_grouped[0][0], cntRate_grouped[0][1], cntRate_grouped[0][0]*cntRate_grouped[0][1], cntRate_grouped[0][1]/cntRate_overall[1], cntRate_grouped[0][2]]]
    else:
        return None
    return highly_positively_predictive_small_population

# based on the high NULL columns and highly predictictive small population suggestions to remove rows
def getRowstoDelete(colNames, data, eventCol, highly_positively_predictive_small_population, nullIssues, params):

    # first rank highly_positively_predictive_small_population by conversion rate:
    highly_positively_predictive_small_population = sorted(highly_positively_predictive_small_population, key = lambda x : x[-2],reverse = True)

    rowsDel = set([])
    idIdx = colNames.index(params.idColumn)
    idCol = [x[idIdx] for x in data]
    event_tot = sum(eventCol)
    # delete rows that are in the group with higher conversion rate until reaching the maximum allowed number
    #highly_positively_predictive_small_population = [x for x in highly_positively_predictive_small_population if x[-1] != 'State']
    for h in highly_positively_predictive_small_population:
        if h[0] not in [j[1] for j in nullIssues] and ffloat(h[-1]) > 2.0:
            colIdx = colNames.index(h[0])
            # print(colIdx)
            if h[1] == 'cat':
                r = set([x[idIdx] for x in data if x[colIdx] == h[2]])
            elif h[1] == 'num':
                r = set([x[idIdx] for x in data if x[colIdx] in ['', None] or math.isnan(ffloat(x[colIdx]))])
                # rowsDel_temp  - just to count the number of rows deleted so far
            rowsDel_temp = rowsDel | r
            # when the number of rows deleted is greater than 1% of total or if the number of event associated with the rows to be deleted is greater than (1% of total events or the number of minimum number of events - 500) - whichever is the minimum, then no more rows to be deleted
            if len(rowsDel_temp) > params.rowDelallowed * len(data) or sum([x[0] for x in zip(eventCol, idCol) if x[1] in rowsDel_temp]) > event_tot*min(params.eventDelallowed, (event_tot-params.eventMinumum)*1.0/event_tot):
                break
            h.append(', '.join(r))
            rowsDel = rowsDel | r
        else:
            continue

    return [x for x in data if x[idIdx] in rowsDel], highly_positively_predictive_small_population

# Calculate the lift by 10 buckets and check model performance in UI
# checks the UI chart based on 3 criteria - top decile lift, bottom decile lift and statistically acceptible lift variation
# input: data(model summary json file data), conversion rate
def liftChart(data, conv, pm, nDeciles = 10, maxLift_lowerdec = 1.0):

    # liftFlags = []

    # get data from the json file
    score = sorted([(x[pm.scoreCol],x[pm.countCol],x[pm.convertedCol]) for x in data],key = lambda y:y[0])

    # set sequence
    seq = [1+(10*j) for j in range(0,nDeciles+1)]

    # calculate lift by bucket
    lift_Bucketed = [(float(sum([y[2] for y in score[seq[i]-1:(seq[i+1]-1)]]))*1/sum([z[1] for z in score[seq[i]:(seq[i+1]-1)]]) *1/conv) for i in range(0,nDeciles)]

    errorMessage = []

    # check if top 3 deciles have lift in descending order OR if any of the last 4 deciles have lift > 1 OR if the lift between consecutive deciles is varying within statistically acceptable limits
    if lift_Bucketed[nDeciles-1] < lift_Bucketed[nDeciles-2] or lift_Bucketed[nDeciles-2] < lift_Bucketed[nDeciles-3]:
        errorMessage.append('Top 3 deciles NOT in descending order')

    # if len([i for i,j in zip(range(nDeciles-3,nDeciles),range(nDeciles-2,nDeciles)) if lift_Bucketed[i]-lift_Bucketed[j] > max(-1.0,-calcStdDev(lift_Bucketed[nDeciles-3:]))]) > 0:
    #     liftFlags.append(('UI','liftChart performance','Failed', 'Top 3 deciles NOT siginificantly different from each other'))
    # else:
    #     liftFlags.append(('UI','liftChart performance','Passed'))

    if max(lift_Bucketed[0:5]) > maxLift_lowerdec:
        errorMessage.append('At Least one of the bottom 5 decile has lift > {}'.format(maxLift_lowerdec))

    if len(errorMessage) == 0:
        errorMessage = ['No Issue']

    return errorMessage

# checks the one-hot encoded features from rf model file against the expected feature list in addition to the regular check
def checkFeature(feat, expFeatlist):

    return any(feat.startswith((i[0])) for i in expFeatlist)

# Check the top features in the rfmodel file where feature importance > 0.05
# Check when one hot encoded NULLs appear in the rfmodel file
# Check the top features that have not appeared with Importance > 0.012 historically
# input: data (rf_model file), e_data (expected features list), upper (highLim) and lower (lowLim) for rf importance
def topFeatures(data, e_data, highLimit, lowLimit):

    topFeatureflags = []

    # check for all features with very high importance
    topFeatureflags.extend([('High Feature Importance', data[i][0], 'Failed' ,data[i][1]) for i in range(0,len(data)) if ffloat(data[i][1]) >= highLimit])

    # check for features with '_NULL'
    topFeatureflags.extend([('NULL feature as Important', data[i][0], 'Failed' ,data[i][1]) for i in range(0,len(data)) if any(x in data[i][0].lower() for x in ['_null', '_isnull']) and ffloat(data[i][1]) >= lowLimit])

    # check if important features exist in the expected feature list
    topFeatureflags.extend([('Unexpected feature with high importance', data[i][0], 'Check feature' ,data[i][1]) for i in range(0,len(data)) if not checkFeature(data[i][0],e_data) and ffloat(data[i][1]) >= lowLimit])

    return [x for x in topFeatureflags if x]

# Check for HG data and BuiltWith features that might end up dominating the model
# input column names - all columns of builtWith and HG Data, rf model (r_data), top predictors (t_data), null definition (n_data), lower (rf_lowLim) for rf importance, domlist - list of dominating feature sources
def dominatingFeatures(r_data, t_data,domlist, n_data, rf_lowLim, liftLimit = 3.0, popPerclimit = 0.2):

    domFeat = []
    # Check if any of the features from dominating sources occur in the rf model file with importance > 0.012
    for i in domlist:
        #if len([x[0] for x in r_data if ffloat(x[1]) >= rf_lowLim and i in x[0]]) > 0:
        domFeat.extend([x[0] for x in r_data if ffloat(x[1]) >= rf_lowLim and i in x[0]])

    # get the population percent and lift for each features from the list above
    dominatingFeat = [(i,max([float(t[7]) for t in t_data if t[0] == i]),max([1-float(x[10]) for x in t_data if x[0] == i and x[5] in [y for y in n_data]])) for i in domFeat]

    # check if lift and population percent criteria are met
    return [(x[0],x[1],x[2]) for x in dominatingFeat if x[1] > liftLimit and x[2] < popPerclimit]

# Calculate nonNull lift
# using the conversion rate = # of conversions/overall population of non NULLs
# data = top predictor file data, rate = full sample conversion rate
def nonNulllift(data, rate):

    try:
        return (sum([round(x[4]*x[5],0) for x in data])/sum([x[5] for x in data]))*1/rate
    except:
        return float('nan')

# Check if nulls are highly negatively predictive so that values sparsely populated becomes highly predictive even if not really important
# Input data rf model file and topo predictor file, NULL definition (n_data), conversion rate and rf importance lower limit
# nullPoppercLimit = limit for null population for this test, liftLimit = lift limit for this test
def nullNegativepred(r_data,t_data, n_data, conv, rf_lowLim, nullPoppercLimit = 0.3, liftLimit = 0.3, avgLiftlimit = 2.0):

    impCols = [x[0] for x in r_data if ffloat(x[1]) > rf_lowLim]

    colBuckets = [(x[0],x[5],ffloat(x[10]),ffloat(x[7]),ffloat(x[6]),ffloat(x[9])) for x in t_data if x[0] in impCols]

    # cols where Null pop > 30% and null lift < 0.3
    testCols = [x[0] for x in colBuckets if x[1] in n_data and x[2] > nullPoppercLimit and x[3] < liftLimit]

    # calculate average lift for non null buckets
    if len(testCols) > 0:
        nonNulllift_nullCols = [('Dominant NULLs/Future Information',y,nonNulllift([z for z in colBuckets if z[0] == y and z[1] not in n_data],conv)) for y in testCols]
        return [x for x in nonNulllift_nullCols if x[2] > avgLiftlimit]

    else:
        return []

# population percent < 2% and lift > 3X
def limitCriteria_1(e_col, data, lim):

    lim_1 = [[1 for y in data if ffloat(y[10]) < ffloat(z[4]) and ffloat(y[7]) > float(z[5]) and y[0] == e_col] for z in lim if z[0] == e_col]

    return 0 if not zip(*lim_1) else 1

# if trend fails more than once
def limitCriteria_2(e_col, data, l_data, null_data, conv):

    sub = [x for x in data if x[0] == e_col and x[5] not in null_data]

    if len(sub) > 2:
        avgLift = [((float(sub[x][6])*float(sub[x][9]))+(float(sub[x-1][6])*float(sub[x-1][9])))/(float(sub[x][9])+float(sub[x-1][9]))/conv for x in [1,len(sub)-1]]
    if len(sub) == 2:
        avgLift = [float(sub[0][6]),float(sub[1][6])]
    else:
        return 0

    if 'Rank' not in e_col:
        return 0 if avgLift[1] > avgLift[0] else 1
    else:
        return 1 if avgLift[1] > avgLift[0] else 0

#    sub = [x for x in data if x[0] == e_col and x[5] not in null_data]
#    diff =  [float(sub[k][7]) - float(sub[j][7]) for j,k in zip(range(0, len(sub)),range(1,len(sub)))]
#
#    cnt = 0
#    # count the number of times the change of sign happens when the difference in lift going one way is calculated
#    if  len(diff) > 1:
#        cnt = [1 for i,j in zip(range(0, len(diff)),range(1,len(diff))) if diff[i] * diff[j] < 1.0].count(1)
#    else:
#        return 0
#
#    return 1 if cnt > 1 else 0

# high lift check > 5X
def limitCriteria_3(e_col, data, l_data):

    lim_3 = [[1 for y in data if ffloat(y[7]) > ffloat(z[8]) and y[0] == e_col] for z in l_data if z[0] == e_col]

    return 0 if not zip(*lim_3) else 1

# Check normal operational limits for numerical variables
# Input top predictor data,rf model data, l_data - limits from the expected feature limits file
def featLimits(r_data, t_data, colType, l_data, n_data, convRate):

    f_cnt = [(i[0],limitCriteria_1(i[0],t_data,l_data),limitCriteria_2(i[0],t_data,l_data,n_data,convRate ),limitCriteria_3(i[0],t_data,l_data)) for i in colType if i[1] == 'num']

    test =  [[x[0], str(x[1]),str(x[2]),str(x[3])] for x in f_cnt if x[1]+x[2]+x[3] > 0]

    return [[r.replace('0','Passed').replace('1','Failed') for r in y] for y in test]

# Calculate conversion rate by group of employee range
# get population percent, lift and alexa matchrate
def groupbyFunc(groupingCol, grpedCol):

    data_grouped = groupby(sorted(zip(groupingCol, grpedCol), key = lambda y: y[0]), lambda z: z[0])

    # get the grouped iterator into a column grouped in a key - value, event pair
    cntRate_grouped = {key : [x for x in group] for key, group in data_grouped}

    #cnt_grouped = Counter(groupingCol)

    return cntRate_grouped

# calculate population percent, matchrate and lift by company size grouping to identify if the training set could be split into subpopulations
# input = train+test event table, eventCol, conversion rate and parameters
# output = Company group, subpopulation lift, match rate difference, group size and group population percent
# check the subpopulation group criteria
# population > 5000 or percentage>15% and
# (lift in sub population > 1.5 or < 0.5 or abs (matching rate in alexa or bw or hg in sub population – matching rate overall) > 10%)

def trainDatasplittingFunction(data,col, colVal_grp, eventCol, matchRate_overall,pm, splitFeatName):

    convRate = sum(eventCol)*1.0/len(eventCol)

    cnt_grouped = Counter(colVal_grp)
    convRate_grouped = groupbyFunc(colVal_grp, eventCol)
    matchRate_grouped = groupbyFunc(
        colVal_grp, [x[col.index(pm.matchRateColumn)]
                     for x in data])

    HGmatchRate_grouped = groupbyFunc(
        colVal_grp, [x[col.index(pm.HGmatchRateColumn)]
                     for x in data])

    BWmatchRate_grouped = groupbyFunc(
        colVal_grp, [x[col.index(pm.BWmatchRateColumn)]
                     for x in data])

    DNBmatchRate_grouped = groupbyFunc(
        colVal_grp, [x[col.index(pm.DNBmatchRateColumn)]
                     for x in data])

    conv_grouped = {k:sum([x[1] for x in v]) for k,v in convRate_grouped.items()}
    matRate_grouped = {k:len([x[1] for x in v if x[1] != '' and not pd.isnull(x[1])])*1.0/len(v) for k,v in matchRate_grouped.items()}
    HGmatRate_grouped = {k:len([x[1] for x in v if x[1] != '' and not pd.isnull(x[1])])*1.0/len(v) for k,v in HGmatchRate_grouped.items()}
    BWmatRate_grouped = {k:len([x[1] for x in v if x[1] != '' and not pd.isnull(x[1])])*1.0/len(v) for k,v in BWmatchRate_grouped.items()}
    DNBmatRate_grouped = {k:len([x[1] for x in v if x[1] != '' and not pd.isnull(x[1])])*1.0/len(v) for k,v in DNBmatchRate_grouped.items()}

    popPerc_grouped = {k: float(cnt_grouped[k])/len(data) for k in cnt_grouped}

    lift_grouped = {k: (float(conv_grouped[k])/cnt_grouped[k])/convRate for k in convRate_grouped}
    testResult = {k: 'no' for k in lift_grouped}
    for k in lift_grouped:
        if (lift_grouped[k] > pm.liftUpperlimit or lift_grouped[k] < pm.liftLowerlimit or abs(matRate_grouped[k] - matchRate_overall) > pm.minMatchratediff) and (cnt_grouped[k] > pm.subPoplowerLimit or popPerc_grouped[k] > pm.subPopminPercent) and conv_grouped[k] > pm.minEventCnt:
            testResult[k] = 'yes'


    return [(splitFeatName, k, lift_grouped[k], matRate_grouped[k],DNBmatRate_grouped[k],BWmatRate_grouped[k],HGmatRate_grouped[k], cnt_grouped[k], popPerc_grouped[k], conv_grouped[k], testResult[k]) for k in lift_grouped]

###########################################################################
# Sriteja's functions below
###########################################################################

#compares lift measures corresponding to the attribute values with in a range
def SpamIndicator_LiftComparison(source_toppredictorfile, Spam_Columns, tp_buckets,
                                 tp_colname, tp_liftcol):
    """
    last 3 arguments
    TopPredictor_Attributebuckets
    TopPredcitor_ColumnName
    TopPredictor_LiftColumn
    """
    output = []
    df = pd.read_csv(source_toppredictorfile)
    for Spam_Column in Spam_Columns:
        #create dictionary with lifts and the column names
        m = dict(zip(df[tp_buckets][df[tp_colname] == Spam_Column],df[tp_liftcol][df[tp_colname] == Spam_Column]))
        #creates a list of tuples sorted by values(lift) from the dict above
        k = sorted(m.items(), key=lambda r: r[1])
        #get the attribute values into a list
        bn = [i[0] for i in k]
        bn = [j.replace('< ', "") for j in bn]
        bn = [j.replace('>= ', "1") for j in bn]
        bn = [float(j) for j in bn]
        '''if the sorted list ascending or descending and the original list are same, 
        it means that the attribute values are increasing or decreasing with lift values(checks for monotonicity)'''
        if not (bn == sorted(bn) or bn == sorted(bn,reverse=True)):
            x = [Spam_Column, 'Lift Comparison Test',Spam_Column,'Failed','Values of Lift and attributes are not in the same order']
            output.append(x)
    return output

#combines traning and test post match event table files
def Combine_TrainTest(training,test):
    df_train = pd.read_csv(training, low_memory = False)
    if os.path.isfile(test):
        df_test = pd.read_csv(test, low_memory = False)
        x = [df_train, df_test]
        df_traintest = pd.concat(x, ignore_index = True)
    else:
        df_traintest = df_train
    return df_traintest

#generates lift of a segment given we know the segment conversion
def Lift_Seg(Segment_Conversion,df_traintest, eventcol):
    """
    eventcol = ColumnName_Event
    """
    Overall_Conversion = np.sum(df_traintest[eventcol])/float(len(df_traintest.index))
    lift_Segment = Segment_Conversion/Overall_Conversion
    return lift_Segment

#Flags if  the ratio of lift of a segment (Combination of COlumn name and value) and the NON-NULL lift of the segment is greater than a certain limit
def Flag_Lift_Segment(Column_name, Column_Value, ColumnName_Event, lift_limit,df_traintest):
    try:
        if Column_name not in df_traintest.columns:
            return []
        TotalRecords_Segment = np.sum(df_traintest[Column_name] == Column_Value)
        TotalRecords_Segment_NonNULL = np.sum(df_traintest[Column_name].notnull())
        if (TotalRecords_Segment > 0 and TotalRecords_Segment_NonNULL > 0):
            Segment_Conversion = np.sum(df_traintest[ColumnName_Event][df_traintest[Column_name] == Column_Value])/float(TotalRecords_Segment)
            Segment_Conversion_NonNULL = np.sum(df_traintest[ColumnName_Event][df_traintest[Column_name].notnull()])/float(TotalRecords_Segment_NonNULL)
            Lift_Ratio = Lift_Seg(Segment_Conversion,df_traintest, ColumnName_Event)/Lift_Seg(Segment_Conversion_NonNULL,df_traintest, ColumnName_Event)
            if round(Lift_Ratio,1) > lift_limit:
                output = [Column_name, 'Ratio of Segment Lift /Non Null Lift',Column_name + '=' + str(Column_Value),'Failed',round(Lift_Ratio,1)]
                return output
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('!! ' + line for line in lines))

    return []

#Flags if lift of NULL values in a column is greater than a certain limit
def Flag_Lift_SegmentIsNull(Column_name, ColumnName_Event, lift_limit,df_traintest):
    if Column_name not in df_traintest.columns:
        return []
    TotalRecords_Segment = len(df_traintest[ColumnName_Event][df_traintest[Column_name].isnull()])
    if (TotalRecords_Segment > 0):
        Segment_Conversion = np.sum(df_traintest[ColumnName_Event][df_traintest[Column_name].isnull()])/float(TotalRecords_Segment)
        if round(Lift_Seg(Segment_Conversion,df_traintest, ColumnName_Event),1) > lift_limit:
            output = [Column_name, 'Lift Segment Measure', Column_name + '= NULL' ,'Failed',round(Lift_Seg(Segment_Conversion,df_traintest, ColumnName_Event),1)]
            return output

#Calculates the lift of a segment for spamindicator columns
def SpamIndicator_LiftSegments(segment_definition, eventcol, lift_limit,eventTable):
    """
    eventcol: ColumnName_Event
    eventTable: EventTable_TrainTest
    """
    output = [Flag_Lift_Segment(k,v, eventcol, lift_limit,eventTable) for k,v in segment_definition.items()]
    output = [x for x in output if x]
    return output

#Calculates the lift of NULL valued rows for internal features
def InternalFeatures_NULLLift(Internal_Features,lift_limit, eventcol, eventTable):
    output = [Flag_Lift_SegmentIsNull(k,eventcol, lift_limit,eventTable) for k in Internal_Features]
    output = [x for x in output if x]
    return output

    #Calculates the lift of HG featrues at 0 value
def FeatureGroup_LiftSegments(lift_limit, eventTable, eventcol, colpatterns,
                              attr_val = 0):
    """
    buid a dictionary with the HG Feature column names(keys) and the value(0)
    for which lift is being calculated
    hgpattern = HGsearch_key
    eventTable = EventTable_TrainTest
    """
    col_list = [k for k in eventTable.columns if \
                sum([k.startswith(pat) for pat in colpatterns]) > 0]
    segment_definition = {col:attr_val for col in col_list}
    output = [Flag_Lift_Segment(k,v, eventcol, lift_limit,eventTable) for k,v \
              in segment_definition.items()]
    output = [x for x in output if x]
    return output

def getCompanySize(sizeStr):
    if pd.isnull(sizeStr) or sizeStr == '':
        return 'Unknown'

    size = sizeStr.replace('-',' ').replace('>',' ').replace('<',' ').replace(',','').split()
    sizeUB = float(size[-1])
    if sizeUB <= 50:
        return 'SMB'
    if sizeUB <= 500:
        return 'MIDMARKET'
    if sizeUB <= 10000:
        return 'ENT'
    return 'Unknown'

def checkColumnExistence(evtTable_data, evtTable_col, params):


    evtTable_col.append(params.dummyCOlName)

    evtTable_colset = set(evtTable_col)

    def getAltName(orig, alt):
        if orig in evtTable_colset:
            return orig
        if alt in evtTable_colset:
            return alt
        return params.dummyCOlName

    params.matchRateColumn = getAltName(params.matchRateColumn, params.matchRateColumnAlt)
    params.HGmatchRateColumn = getAltName(params.HGmatchRateColumn, params.HGmatchRateColumnAlt)
    params.BWmatchRateColumn = getAltName(params.BWmatchRateColumn, params.BWmatchRateColumnAlt)
    params.DNBmatchRateColumn = getAltName(params.DNBmatchRateColumn, params.DNBmatchRateColumnAlt)

    params.publicDomain = params.publicDomain if params.publicDomain in evtTable_colset else params.dummyCOlName
    params.emailColumn = [x if x in evtTable_colset else params.dummyCOlName for x in params.emailColumn]
    params.companyColumn = getAltName(params.companyColumn, params.companyColumnAlt)
    params.countryColumns = [x if x in evtTable_colset else params.dummyCOlName for x in params.countryColumns]

    for key in params.source_et.keys():
        if key not in evtTable_colset:
            del params.source_et[key]

def trendIdentification(e_col, data, null_data):

    sub = [[x[0],float(x[6]),float(x[9])] for x in data if x[0] == e_col and x[5] not in null_data]
    if len(sub) > 2:
        sampleSize = float(data[0][8])
        i = 0
        while i < len(sub) and len(sub) > 1:
            if sub[i][2]/sampleSize < 0.01:
                if i == 0 or (i < len(sub)-1 and sub[i+1][2] < sub[i-1][2]):
                    sub[i+1][1] = (sub[i+1][1]*sub[i+1][2] + sub[i][1]*sub[i][2])/(sub[i+1][2] + sub[i][2])
                    sub[i+1][2] = sub[i+1][2] + sub[i][2]
                    del sub[i]
                elif i == len(sub)-1 or sub[i+1][2] > sub[i-1][2]:
                    sub[i-1][1] = (sub[i-1][1]*sub[i-1][2] + sub[i][1]*sub[i][2])/(sub[i-1][2] + sub[i][2])
                    sub[i-1][2] = sub[i-1][2] + sub[i][2]
                    del sub[i]
            else:
                i = i + 1

    trendInd = [sub[i][1] >= sub[i-1][1] for i in range(1, len(sub))]

    if len(trendInd) == 0:
        return 'noTrend'

    if sum(trendInd) == len(trendInd):
        return 'upTrend'

    if sum(trendInd) == 0:
        return 'downTrend'

    return 'noTrend'


# function that calls all the functions generating flags
def hdfsModelReviewReport( modelName, modelDirectoryPath, trainingFilePath, configParametersPath):

    logger.info("Attempting to generate model quality report for model: " + modelName)
    logger.info("Model Directory: " + modelDirectoryPath)
    param = hdfsRevParam( modelName, modelDirectoryPath, trainingFilePath, configParametersPath)
    sampleMatchrate = []
    modelQuality = []
    topFeaturebehavior = []
    nullIssues = []
    featureLimits = []
    posPredsmallPop = []
    sampleSplitting = []

    highly_positively_predictive_small_population = {}
    sheet = 1

    tp_col, tp_data = ModelingEnvironment.get_instance().load_csv_file(param.topPredictor_file)

    aucScore, liftScore, rate, colNameList = ModelingEnvironment.get_instance().load_json_file(param.modelsummaryJson_file)

    rf_col,rf_data = ModelingEnvironment.get_instance().load_csv_file(param.rfmodel_file)

    ex_col, ex_data = ModelingEnvironment.get_instance().load_csv_file(param.expectFeat_file)

    lim_col, lim_data = ModelingEnvironment.get_instance().load_csv_file(param.expectFeatLimit_file)

    evtTable_data, evtTable_col = loadEventtable(param.trainDataFile,param.testDataFile)

    countryGroup_col, countryGroup_data = ModelingEnvironment.get_instance().load_csv_file(param.countryGrouping_file)

    evtTable_data = [x + [None] for x in evtTable_data]
    checkColumnExistence(evtTable_data, evtTable_col, param)
    #score_col, score_data = ModelingEnvironment.get_instance().load_csv_file(param.scoreFile)

    colNameList, colTypeList = getColTypes(param, evtTable_data, evtTable_col, colNameList, rf_data)
    eventCol = [x[evtTable_col.index(param.eventColName)] for x in evtTable_data]
    eventCol = [1 if str(x).lower() in ['true', '1','1.0'] else 0 for x in eventCol]
    cntRate_overall = (len(evtTable_data), getRate(eventCol))
    emailCol = [x for x in param.emailColumn if x in evtTable_col][0]

    overAllTab = []
    overAllTab.append(['AUC', round(aucScore,3)])

    match_rate, matchPr_rate = matchRate(param,tp_data, evtTable_data, evtTable_col)
    overAllTab.extend(sorted([[' '.join(['Match Rate - ', x]), round(y,3)] for x, y in match_rate.items()], key=lambda x: x[0]))

    if matchPr_rate is not None:
        overAllTab.extend(sorted([[' '.join(['Match Rate (private domain) - ', x]), round(y,3)] for x, y in matchPr_rate.items()], key=lambda x: x[0]))

    domainCnt, dupDomainPop, dupDomainLift= emailDomaincheck(evtTable_data, evtTable_col, param.publicDomain, emailCol, eventCol)

    if domainCnt is not None:
        overAllTab.append(['multiple leads per domain', 'population % = {0}%, lift = {1}'.format(round(dupDomainPop*100,1), round(dupDomainLift,3)), ' '.join([''.join(['[',str(x[0]),'-',str(x[1]),']']) for x in domainCnt])])
    else:
        overAllTab.append(['multiple leads per domain', 'no duplicates on the business domains'])

    domainCntryCnt, dupDomainCntryPop, dupDomainCntryLift = dedupCheck(evtTable_data, evtTable_col, param.publicDomain, emailCol, param.leCountryColName, param.countryColName, eventCol)
    if domainCntryCnt is not None:
        overAllTab.append(['multiple leads per domain&country', 'population % = {0}%, lift = {1}'.format(round(dupDomainCntryPop*100,1), round(dupDomainCntryLift,3)), ' '.join([''.join(['[',str(x[0]),'-',str(x[1]),']']) for x in domainCntryCnt])])
    else:
        overAllTab.append(['multiple leads per domain&country', 'no duplicates on the business domains&country'])


    companyCnt,dupCompanyPop, dupCompanyLift = leadsperCompany(evtTable_data, evtTable_col, param.companyColumn, eventCol)

    if companyCnt is not None:
        overAllTab.append(['multiple leads per company', 'population % = {0}%, lift = {1}'.format(round(dupCompanyPop*100,1), round(dupCompanyLift,3)), ' '.join([''.join(['[',str(x[0]),'-',str(x[1]),']']) for x in companyCnt])])
    else:
        overAllTab.append(['multiple leads per company', 'no duplicates on the company names'])


    toplvlDomain = toplvlDomaincheck(evtTable_data, evtTable_col, param.publicDomain, emailCol, eventCol)

    if toplvlDomain is not None:
        overAllTab.append(['Top {} top-level-domains'.format(len(toplvlDomain)), ', '.join([x[0] for x in sorted(toplvlDomain.items(), key = lambda z: z[1][0], reverse = True)]), ', '.join(['-'.join([x, '{0}% {1}x'.format(round(y[0]*100,0), round(y[1],2))]) for x, y in sorted(toplvlDomain.items(), key = lambda z: z[1][0], reverse = True)])])


    # print ' | '.join(['-'.join([x, '{0}% {1}x'.format(round(y[0]*100,0), round(y[1],2)) for x, y in toplvlDomain.items()]) ])

    pubDomainTuple = pubDomainpop(evtTable_data, evtTable_col, eventCol, param.publicDomain)

    if pubDomainTuple is not None:
        overAllTab.append(['public domain', 'population % = {0}%, lift = {1}'.format(round(pubDomainTuple[0]*100,1), round(pubDomainTuple[1],3))])

    overAllTab.append(['lift chart', ' | '.join(liftChart(liftScore,rate, param))])

    sampleSplitting = []

    matchRate_overall = len([x[evtTable_col.index(param.matchRateColumn)] for x in evtTable_data if x[evtTable_col.index(param.matchRateColumn)] != ''])/float(len(evtTable_data))

    # Company size grouping
    try:
        if param.employeeRangecolumn in evtTable_col:
            empSize = [getCompanySize(x[evtTable_col.index(param.employeeRangecolumn)]) for x in evtTable_data]
            empSize_splitting = trainDatasplittingFunction(evtTable_data,evtTable_col,empSize, eventCol,matchRate_overall,param, 'EmployeeSize')
            sampleSplitting.extend(empSize_splitting)
    except:
        empSize_splitting = []
        print("Could not extract company size")

    # Region grouping
    countryGroup_data = {x[0].lower():x[1] for x in countryGroup_data}

    countryColName = param.countryColumns[0] if param.countryColumns[0] in evtTable_col else param.countryColumns[1]
    countryCol = ['' if pd.isnull(x[evtTable_col.index(countryColName)]) else x[evtTable_col.index(countryColName)].lower()  for x in  evtTable_data]
    region_grp = [countryGroup_data[x] if x in countryGroup_data else 'Unknown' for x in countryCol]

    # only when more than one country group (GEO) is present, a flag is generated
    if len(Counter(region_grp)) > 1:
        region_splitting = trainDatasplittingFunction(evtTable_data,evtTable_col,region_grp, eventCol,matchRate_overall,param, 'Region')
        sampleSplitting.extend(region_splitting)

    try:
        if param.employeeRangecolumn in evtTable_col:
            overAllTab.append(['Lifts of companies of different sizes', ' '.join(['[{0} - {1}% {2}x] '.format(x[1], round(x[8]*100,1), round(x[2],1)) for x in empSize_splitting])])
    except:
        print("Could not manage company sizes")

    wrappedWB = WrappedExcelFile()

    wrappedWB.printOutResults(
                    overAllTab,
                    ['', 'Value/Description', 'Additional Info'],
                    'OverAll',
                    set_width=True)

    wrappedWB.printOutResults(sampleSplitting,
                    ['Criteria',  'Group', 'Lift',
                     'Match Rate (AlexaRank)', 'Match Rate (DNB)', 'Match Rate (BW)',
                     'Match Rate (HG)',
                     'Count','Population Percent', '# of Positive Events', 'Recommended Split'],
                    'sampleSplitting',
                    set_width=True)

    rfModelData = {x[0]: float(x[1]) for x in rf_data}
    colTypeDict = dict(zip(colNameList, colTypeList))

    # flags from highly positively predictive small population
    for colName in rfModelData:
        if rfModelData[colName] < param.featureImp_lowerLim:
            continue

        if colName.startswith('DS_') or colName.endswith('Grouped'):
            continue

        if colName in colNameList:
            origColName = colName
        else:
            # one hot encoder
            try:
                origColName = [x for x in colNameList if colName.startswith(x)][0]
            except IndexError:
                print("{} doesn't exist in the event table. Report it to the data science team".format(colName))

        colType = colTypeDict[origColName]

        colVal = getColVal(evtTable_data, evtTable_col.index(origColName), colType, param.numBucket)

        cntRate_grouped = getGroupedRate(colVal, eventCol, cntRate_overall)

        highly_positively_predictive_small_population.update({origColName : get_highly_positively_predictive_small_population(cntRate_grouped, cntRate_overall, colType, param)})

    highly_positively_predictive_small_population = flatten_dict(highly_positively_predictive_small_population)

    # get rows to be deleted
    rowsTodelete, highly_positively_predictive_small_population_2  = getRowstoDelete(evtTable_col, evtTable_data, eventCol, highly_positively_predictive_small_population, nullIssues, param)

    posPredsmallPop.extend(highly_positively_predictive_small_population_2)

    evtTable_df = pd.DataFrame(data=evtTable_data, columns = evtTable_col)

    fnames = [r[0] for r in rf_data]
    fimp = [r[1] for r in rf_data]
    highLimit, lowLimit = param.featureImp_higherLim,param.featureImp_lowerLim
    fimp_high = [ffloat(imp) >= highLimit for imp in fimp]
    fnullimp = [(r[0].lower().endswith('_null') or r[0].lower().endswith('_isnull__')) and
                ffloat(r[1]) >= lowLimit for r in rf_data]
    funexp = [(not checkFeature(r[0],ex_data)) and ffloat(r[1]) >= lowLimit for r in rf_data]
    high_bw_hg = dominatingFeatures(rf_data, tp_data, param.dom_Features,
                                    param.no_data,param.featureImp_lowerLim)
    high_bw_hg_features = [x[0] for x in high_bw_hg]
    f_bwhg_high = [fname in high_bw_hg_features for fname in fnames]

    nullneg = nullNegativepred(rf_data, tp_data, param.no_data, rate,
                               param.featureImp_lowerLim)
    nullneg_features = [x[0] for x in nullneg]
    f_internalnullneg = [fname in nullneg_features and fname in param.Int_Feat for fname in fnames]

    nullLiftlimit = 1.02
    #nullPopLimit = 0.05
    # check cols with < 95% Null and Lift > 1.02 for rows removal
    # since threshold is the same no matter
    #    nulllow_features = [x[0] for x in tp_data if x[5] in param.no_data and
    #                        float(x[10]) < (1-nullPopLimit) and float(x[7]) > nullLiftlimit]
    #    f_nulllow = [fname in nulllow_features for fname in fnames]
    #    nullhigh_features = [x[0] for x in tp_data if x[5] in param.no_data and
    #                         float(x[10]) > (1-nullPopLimit) and float(x[7]) > nullLiftlimit]
    #    f_nullhigh = [fname in nullhigh_features for fname in fnames]
    nullpos_features = [x[0] for x in tp_data if x[5] in param.no_data and
                        float(x[7]) > nullLiftlimit]
    f_nullpos = [fname in nullpos_features for fname in fnames]
    impCols = [x for x in zip(colNameList, colTypeList) if x[0] in [r[0]
                                                                    for r in rf_data if ffloat(r[1]) > param.featureImp_lowerLim]]
    limits_out = featLimits(rf_data, tp_data, impCols, lim_data, param.no_data, rate)

    #    smallpophighlift_features = [x[0] for x in limits_out if x[1]=='Failed']
    #    badtrend_features = [x[0] for x in limits_out if x[2]=='Failed']
    highlift_features = [x[0] for x in limits_out if x[3]=='Failed']
    #    f_smallpophighlift = [fname in smallpophighlift_features for fname in fnames]
    #    f_badtrend = [fname in badtrend_features for fname in fnames]
    f_highlift = [fname in highlift_features for fname in fnames]
    #    smallpred_features = set([x[0] for x in highly_positively_predictive_small_population])
    #    f_smallpophighpred = [fname in smallpred_features for fname in fnames]

    badBW = FeatureGroup_LiftSegments(param.lift_limit_BWData, evtTable_df,
                                      param.eventCol,
                                      [param.BW_key, param.Alexa_key, param.Semrush_key])
    badHG = FeatureGroup_LiftSegments(param.lift_limit_HGData, evtTable_df,
                                      param.eventCol,
                                      [param.HG_key])
    badHPA = FeatureGroup_LiftSegments(param.lift_limit_FeatureTerm, evtTable_df,
                                       param.eventCol,
                                       [param.FeatureTerm_Key, param.HPANum_key])
    badSpam = SpamIndicator_LiftSegments(param.SpamIndicators_LiftSeg,
                                         param.lift_limit_SpamIndicators,
                                         param.eventCol,
                                         evtTable_df)
    badGroup_Features = [x[0] for x in badBW] + [x[0] for x in badHG] + \
                        [x[0] for x in badHPA] + [x[0] for x in badSpam]
    f_badgroup = [fname in badGroup_Features for fname in fnames]
    internalNulls = InternalFeatures_NULLLift(param.Int_Feat,
                                              param.lift_limit_InternalFeatures,
                                              param.eventCol,
                                              evtTable_df)
    internalnull_features = [x[0] for x in internalNulls]
    f_internalNullPos = [fname in internalnull_features for fname in fnames]

    feature_removals = zip(fnames, fimp, fnullimp, f_internalnullneg,
                           f_internalNullPos, f_nullpos)

    feature_removals = [fm for fm in feature_removals if sum(fm[2:])>0]
    feature_removals = [[el if el else '' for el in fm] for fm in feature_removals]

    # features to remove tab
    wrappedWB.printOutResults(feature_removals,
                    ['Feature', 'Importance', 'Null Feature Too Important', 'Internal Null Lift Too Low',
                     'Internal Null Lift Too High', 'Null Lift Too High'],
                    'FeaturesToRemove',
                    set_width=True)

    colTypeDict = dict(zip(colNameList, colTypeList))

    fTrend = [trendIdentification(x, tp_data, param.no_data) if x in colTypeDict and colTypeDict[x] == 'num' else '' for x in fnames]
    feature_warnings = zip(fnames, fimp, fTrend, fimp_high, funexp, f_bwhg_high,
                           f_highlift, f_badgroup)

    feature_warnings = [[el if el else '' for el in fw] for fw in feature_warnings]
    #print feature_warnings

    wrappedWB.printOutResults(feature_warnings,
                    ['Feature', 'Importance', 'Trend', 'Importance Too High', 'Not Usual Top Feature',
                     'BW or HG too High', 'Top Lift Too High',
                     'Lift of 0 Value Exceeds Non-Null Significantly'],
                    'FeatureWarnings',
                    set_width=True)


    posPredsmallPop = [ppsp for ppsp in posPredsmallPop if any([ppsp[0] in f for f in fnames[:25]])]
    posPredsmallPop = [ppsp for ppsp in posPredsmallPop if ppsp[-2] > 2]
    posPredsmallPop = [ppsp for ppsp in posPredsmallPop if ppsp[3] < 1000]
    featureToremove = set([x[0] for x in feature_removals])
    posPredsmallPop = [ppsp for ppsp in posPredsmallPop if not any([ppsp[0] in x for x in featureToremove])]
    wrappedWB.printOutResults(posPredsmallPop,
                    ['Feature','dataType','value','cnt',
                     'Conv. rate','eventCnt','lift','significance', 'Id of Records'],
                    'RowsToRemove',
                    set_width=True)
    internalFeatReport = [(x[0], x[5], x[10], x[7]) for x in tp_data if x[2].lower() == 'lead information' and float(x[10]) > param.interFeatPopshrd and (float(x[7]) > param.interFeatLiftUB or float(x[7]) < param.interFeatLiftLB)]

    wrappedWB.printOutResults(internalFeatReport,
                    ['Feature','Value','population %','lift'],
                    'InternalFeatures',
                    set_width=True)

    textOut = save_virtual_workbook(wrappedWB.wb)
    ModelingEnvironment.get_instance().writeDataToHDFS(param.output_file, textOut)

    ModelingEnvironment.get_instance().onShutdown()

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: hdfsmodelreviewreport.py modelName modelDirectoryPath trainingFilePath configDirectoryPath")
        sys.exit(1)

    hdfsModelReviewReport(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] )
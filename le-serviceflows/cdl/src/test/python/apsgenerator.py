import operator
import pandas as pd
import numpy as np
import logging
import os
import shutil

from apsdataloader import ApsDataLoader
 
logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='ApsGenerator')

def parsePeriods(data, periodCol, periodKey, periodParam):
    periods = [d[periodCol] for d in data]
    maxPid, minPid = max(periods), min(periods)
    if periodKey == 'current' and periodParam is None:    
        minp, maxp = 0, 0
    elif periodKey == 'ever' and periodParam is None:        
        minp, maxp = 0, maxPid - minPid
    elif periodKey == 'between' and type(periodParam) == list:
        minp, maxp = min(periodParam), max(periodParam)
    elif periodKey == 'prior' and type(periodParam) == int:
        minp, maxp = periodParam, maxPid - minPid
    elif periodKey == 'within' and type(periodParam) == int:
        minp, maxp = 0, periodParam
    elif periodKey == 'future' and type(periodParam) == list:
        minp, maxp = min(periodParam), max(periodParam)
    else:
        print 'invalid period arguments'
        return None
    return minp, maxp

# generate list of list of values (one for each APS row) to aggregate
def getShiftedList(data,
                  acctCol,
                  periodCol,
                  prodCol,
                  periodKey,
                  periodParam):
    # output = [[]] * len(data)
    accts = [d[acctCol] for d in data]
    amts = [d[prodCol] for d in data]

    # parse period arguments
    minp, maxp = parsePeriods(data, periodCol, periodKey, periodParam)
    
    # find minimum and maximum row of each account
    minPos = {d[acctCol]:i for i, d in enumerate(data) if i == 0 or data[i][acctCol] != data[i - 1][acctCol]}
    maxPos = {d[acctCol]:i for i, d in enumerate(data) if i == len(data) - 1 or data[i][acctCol] != data[i + 1][acctCol]}
    # return list of values to aggregate for each row
    if periodKey == 'future':
        shifted = [amts[min(maxPos[accts[i]], i + minp) : min(maxPos[accts[i]], i + maxp + 1)] for i in range(len(data))]        
    else:
        shifted = [amts[max(minPos[accts[i]], i - maxp) : max(minPos[accts[i]], i - minp + 1)] for i in range(len(data))]
    return shifted

def getRollingsum(data,
                  acctCol,
                  periodCol,
                  prodCol,
                  periodKey='between',
                  periodParam=[0, 5]):
    priorList = getShiftedList(data, acctCol, periodCol, prodCol, periodKey, periodParam)
    if priorList is None:
        return None
    numP = abs(periodParam[1] - periodParam[0]) + 1
    return [sum(0 if y is None else y for y in inputL) if len(inputL) == numP else np.nan for inputL in priorList]

def getMomentum(data,
                acctCol,
                periodCol,
                prodCol,
                periodKey='between',
                periodParam=[0, 2]):
    priorList1 = getShiftedList(data, acctCol, periodCol, prodCol, periodKey, periodParam)
    periodParam2 = [x + 1 for x in periodParam]
    priorList2 = getShiftedList(data, acctCol, periodCol, prodCol, periodKey, periodParam2)
    if priorList1 is None or priorList2 is None:
        return None
    numerators = [sum([0 if x is None else x for x in inputL]) for inputL in priorList1]
    denominators = [sum([0 if x is None else x for x in inputL]) for inputL in priorList2]
    return [1.0 * n / d - 1 if n != 0 and d != 0 else np.nan for n, d in zip(numerators, denominators)]

def getSpan(data,
            acctCol,
            periodCol,
            prodCol,
            periodKey='ever',
            periodParam=None):
    # span: for each (acct, period, prod), span=3/(3+ current periodid - last periodid 
    # when prod id purchase by acct)
    # when prod has never been purchased, span = 0
    # just purchased in this period: span = 1, last period: 3/4, then 3/5, 3/6 etc.
    priorList = getShiftedList(data, acctCol, periodCol, prodCol, periodKey, periodParam)
    if priorList is None:
        return None
    prdSincePur = [getLastPurPeriod(inputL) for inputL in priorList]
    return [3 / (3 + prds) if prds is not None else 0 for prds in prdSincePur]

def getLastPurPeriod(amountL):
    if len(amountL) == 0 or all([amt is None for amt in amountL]):
        return None
    else:
        p = 0
        while amountL.pop() is None:
            p = p + 1
        return p

# apply operator on a list of input for amount/unit use case
def applyOperator(inputList, engageKey, operatorKey, aggregationKey, compareToVal, numP):
    ops = {'>': operator.gt,
       '<': operator.lt,
       '>=': operator.ge,
       '<=': operator.le,
       '=': operator.eq}
    if engageKey == 'engaged':
        return any([y is not None for y in inputList])
    if engageKey == 'not engaged':
        return all([y is None for y in inputList])
    if engageKey in ['amount', 'unit']:
        if not inputList:
            returnval = False
        elif aggregationKey == 'total':
            returnval = ops[operatorKey](sum(0 if y is None else y for y in inputList), compareToVal)
        elif aggregationKey == 'average':
            if engageKey == 'amount':
                returnval = ops[operatorKey](1.0 * sum([0 if y is None else y for y in inputList]) / len(inputList), compareToVal)
            else:  # cast to int to compare as unit
                returnval = ops[operatorKey](int(sum([0 if y is None else y for y in inputList]) / len(inputList)), compareToVal)
        elif aggregationKey == 'each':
            returnval = all([ops[operatorKey](0 if y is None else y, compareToVal) for y in inputList])
        elif aggregationKey == 'any':
            returnval = any([ops[operatorKey](0 if y is None else y, compareToVal) for y in inputList])
        return returnval
            
def buildAnalyticPurchaseState(transactionDf, numPeriodsAdded=2):
    # build a table with pivoted unit and amount cols for each product
    # this is a skinny version of the current analytic purchase state
    # the current version has features like rolling sum and momentum added in
    # after migration, we can handle these additional features at a different place
    
    # todos
    # confirm all transaction period ids are active
    # confirm all transaction account ids are active
    # confirm all transaction product ids are active and analytic
    # swtich to offset when not all periodIDs are continuous and monotonous
    
    # take only relevant columns
    transactionS = transactionDf[['Account_ID', 'Period_ID', 'Product_ID', 'Amount', 'Quantity']]
    # rename to conform to event table column names
    transactionS.columns = ['LEAccount_ID', 'Period_ID', 'Product_ID', 'Revenue', 'Units']
    # set index, include product_id because pivot doesn't work with multiindex yet
    transactionS = transactionS.set_index(['LEAccount_ID', 'Period_ID', 'Product_ID'])

    # unstack product_id columns, using nan as a filler
    # this is equivalent to keeping account and period ids as index, and pivot on product id
    transactionP = transactionS.unstack('Product_ID', float('nan'))
    # now we need to add intermediate periods even if there is no purchase
    maxP = max(transactionP.index.get_level_values('Period_ID'))
    minP = min(transactionP.index.get_level_values('Period_ID'))
    minPsDf = transactionDf[['Account_ID', 'Period_ID']].groupby(by='Account_ID').min()
    minPs = minPsDf['Period_ID'].to_dict()
    newIndex = []
    for a, p in minPs.items():
        p = p - numPeriodsAdded
        while p <= maxP:
            if p >= minP:
                newIndex.append((a, p))
            p = p + 1
    transactionP = transactionP.reindex(pd.MultiIndex.from_tuples(newIndex, names=['LEAccount_ID', 'Period_ID']))
    # rename columns to playmaker style
    transactionP.columns = transactionP.columns.map('Product_{0[1]}_{0[0]}'.format)
    # sort by account id and period id
    transactionP.sort_index(axis=0, level=[0, 1], inplace=True, ascending=True)
    # put accountId and periodId back as columns
    apState = transactionP.reset_index(level=[0, 1], inplace=False)
    
    # we also need a different format of the same apState table to work with 
    # earlier functions that didn't assume pandas input
    # replace NaN with None because filtering functions assume this
    apStateFill = apState.where((pd.notnull(apState)), None)
    # convert pandas dataframe into a list(index = rowid) of dicts(key=colname)
    apStateDict = apStateFill.T.to_dict().values()
 
    # add 6 period rolling sum, 3 period momentum and span for each product
    # these can be added to analytic purchase state, no need to recreate for each play
    for prodID in transactionDf['Product_ID'].unique():
        rsums = getRollingsum(apStateDict, 'LEAccount_ID', 'Period_ID', 'Product_' + str(prodID) + '_Revenue')
        mmtum = getMomentum(apStateDict, 'LEAccount_ID', 'Period_ID', 'Product_' + str(prodID) + '_Revenue')
        spans = getSpan(apStateDict, 'LEAccount_ID', 'Period_ID', 'Product_' + str(prodID) + '_Revenue')
        apState['Product_' + str(prodID) + '_RevenueRollingSum6'] = rsums
        apState['Product_' + str(prodID) + '_RevenueMomentum3'] = mmtum
        apState['Product_' + str(prodID) + '_Span'] = spans

    return apState, apStateDict

def createAps(transactionDf):
    logger.info("Start to create Aps.")
    apState, apStateDict = buildAnalyticPurchaseState(transactionDf)
    logger.info("Finished creating Aps.")
    return apState

    
if __name__ == '__main__':
    if not os.path.isdir('./input'):
        os.mkdir("./input")
    if not os.path.isdir('./output'):
        os.mkdir("./output")
     
    loader = ApsDataLoader()
    loader.downloadToLocal()
    df = loader.readDataFrameFromAvro()
    logger.info(df.shape)
     
    apState = createAps(df)
    loader.writeDataFrameToAvro(apState)
    logger.info(apState.shape)
    loader.uploadFromLocal()
    

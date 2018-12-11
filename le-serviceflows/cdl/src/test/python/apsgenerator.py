from __future__ import division
from collections import deque
import pandas as pd
import numpy as np
import logging
import os
import shutil
import uuid

from apsdataloader import ApsDataLoader
logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='ApsGenerator')

def getRollingsum(newacct, 
                  amts,
                  isna,
                  winlen):
    rlsm = [np.nan] * len(amts)
    dq = deque(maxlen=winlen)
    for i, a in enumerate(amts):
        if newacct[i]:
            dq.clear()
        dq.append(0 if isna[i] else a)
        if len(dq) == winlen:
            rlsm[i] = sum(dq)
    return rlsm

def getMomentum(newacct, 
                amts,
                isna,
                winlen):
    mmtm = [np.nan] * len(amts)
    dq = deque(maxlen=winlen)
    j = 0
    for i, a in enumerate(amts):
        if newacct[i]:
            dq.clear()
            j = 0
        dq.append(0 if isna[i] else a)
        dqsum = sum(dq)
        d = dqsum - dq[-1]
        n = dqsum - dq[0]
        if j > (winlen - 2) and n!=0 and d!=0:
            mmtm[i] = n / d - 1
        j = j + 1
    return mmtm

def getSpan(newacct, 
            amts,
            isna):
    # span: for each (acct, period, prod), span=3/(3+ current periodid - last periodid 
    # when prod id purchase by acct)
    # when prod has never been purchased, span = 0
    # just purchased in this period: span = 1, last period: 3/4, then 3/5, 3/6 etc.
    span = []
    for i, a in enumerate(amts):
        if newacct[i]:
            if isna[i]:
                currOffs = np.Inf
            else:
                currOffs = 0
        else: # not the first row
            if isna[i]:
                currOffs = currOffs + 1
            else:
                currOffs = 0
        span.append(3 / (currOffs + 3))
    return span
                
def createAnalyticPurchaseState(transactionDf, numPeriodsAdded=2, allowZeroOrNegativeValues=False):
    # build a table with pivoted unit and amount cols for each product
    # this is a skinny version of the current analytic purchase state
    # the current version has features like rolling sum and momentum added in
    # after migration, we can handle these additional features at a different place
    
    #todos
    #confirm all transaction period ids are active
    #confirm all transaction account ids are active
    #confirm all transaction product ids are active and analytic
    #swtich to offset when not all periodIDs are continuous and monotonous
    
    #take only relevant columns
    trxnSlim = transactionDf[['Account_ID', 'Period_ID', 'Product_ID', 'Amount', 'Quantity']]
    #rename to conform to event table column names
    trxnSlim.columns = ['LEAccount_ID', 'Period_ID', 'Product_ID', 'Revenue', 'Units']
    #set index, include product_id because pivot doesn't work with multiindex yet
    trxnSlim = trxnSlim.drop_duplicates(['LEAccount_ID', 'Period_ID', 'Product_ID'])
    apState = trxnSlim.set_index(['LEAccount_ID', 'Period_ID', 'Product_ID'])

    #unstack product_id columns, using nan as a filler
    #this is equivalent to keeping account and period ids as index, and pivot on product id
    apState = apState.unstack('Product_ID', float('nan'))
    
    #now we need to add intermediate periods even if there is no purchase
    maxPid = trxnSlim['Period_ID'].max()
    minPid = trxnSlim['Period_ID'].min()
    minPsDf = trxnSlim[['LEAccount_ID', 'Period_ID']].groupby(by='LEAccount_ID').min()
    minPs = minPsDf['Period_ID'].to_dict()
    newIndex = []
    for a, p in minPs.items():
        p = p - numPeriodsAdded
        while p <= maxPid:
            if p >= minPid:
                newIndex.append((a, p))
            p = p + 1
    #insert periods without any transactions
    apState = apState.reindex(pd.MultiIndex.from_tuples(newIndex, names=['LEAccount_ID', 'Period_ID']))
    #sort by account id and period id
    apState.sort_index(axis=0, level=[0, 1], inplace=True, ascending=True)

    accts = apState.index.get_level_values('LEAccount_ID')
    newacct = [i==0 or accts[i] != accts[i-1] for i in range(len(accts))]
    #perds = apState.index.get_level_values('Period_ID')
    #minPos={a:i for i, a in enumerate(accts) if i==0 or accts[i]!=accts[i-1]}
    # add 6 period rolling sum, 3 period momentum and span for each product
    for prodID in transactionDf['Product_ID'].unique():
        logger.info("Populating Aps for Product Id:" + str(prodID))
        amts = apState['Revenue', prodID].tolist()
        isna = apState['Revenue', prodID].isnull().tolist()
        apState['RevenueRollingSum6', prodID] = getRollingsum(newacct, amts, isna, winlen=6)
        apState['RevenueMomentum3', prodID] = getMomentum(newacct, amts, isna, winlen=4)
        notpurchased = isna
        if not allowZeroOrNegativeValues:
            units = apState['Units', prodID].tolist()
            notpurchased = [testPurchase(amts[i], units[i]) for i in range(len(amts))]
        apState['Span', prodID] = getSpan(newacct, amts, notpurchased)

    if not allowZeroOrNegativeValues:
        apState[apState[['Revenue', 'Units']] < 0] = 0

    # rename columns to playmaker style
    logger.info("Starting to map column names")
    apState.columns = apState.columns.map('Product_{0[1]}_{0[0]}'.format)
        
    #put accountId and periodId back as columns
    logger.info("Starting to re-index")
    apState = apState.reset_index(level=[0,1], inplace=False)
    
    return apState


def testPurchase(amount, unit):
    return isNullOrZeroOrNegative(amount) and isNullOrZeroOrNegative(unit)


def isNullOrZeroOrNegative(value):
    return pd.isnull(value) or value <= 0


def createAps(transactionDf, allowZeroOrNegativeValues=False):
    logger.info("Start to create Aps.")
    apState = createAnalyticPurchaseState(transactionDf, allowZeroOrNegativeValues=allowZeroOrNegativeValues)
    logger.info("Finished creating Aps.")
    return apState

    
if __name__ == '__main__':
    uid = str(uuid.uuid4())
    input = '/mnt/ebs/input' + uid
    output = '/mnt/ebs/output' + uid
    if not os.path.isdir(input):
        os.makedirs(input)
    if not os.path.isdir(output):
        os.makedirs(output)
     
    loader = ApsDataLoader()
    loader.downloadToLocal(input)
    df = loader.readDataFrameFromAvro(input)
    logger.info("df type:" + str(type(df)))
    logger.info("df shape:" + str(df.shape))
    logger.info("df memoryn usage:" + str(df.memory_usage().sum()))
    df.rename(columns={'AccountId':'Account_ID', 'PeriodId':'Period_ID', 'ProductId':'Product_ID',
                       'TotalAmount':'Amount', 'TotalQuantity':'Quantity' }, inplace=True) 
 
    apState = createAps(df)
    logger.info("aps type:" + str(type(apState)))
    logger.info("aps shape:" + str(apState.shape))
    logger.info("aps memoryn usage:" + str(apState.memory_usage().sum()))
    #logger.info("aps density:" + str(apState.density))
    
    apState.insert(0, 'AnalyticPurchaseState_ID', range(len(apState)))
    loader.parallelWriteDataFrameToAvro(apState, output)
    logger.info(apState.shape)
    #loader.uploadFromLocal(output)
    loader.parallelUploadFromLocal(output)
    
    shutil.rmtree(input, ignore_errors=True)
    shutil.rmtree(output, ignore_errors=True)
    

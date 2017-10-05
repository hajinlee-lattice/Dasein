from __future__ import division
import operator
import pandas as pd
import numpy as np

def parsePeriods(data, periodCol, periodKey, periodParam):
    periods=[d[periodCol] for d in data]
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

#generate list of list of values (one for each APS row) to aggregate
def getShiftedList(data, 
                  acctCol,
                  periodCol,
                  prodCol,
                  periodKey,
                  periodParam):
    #output = [[]] * len(data)
    accts = [d[acctCol] for d in data]
    amts = [d[prodCol] for d in data]

    #parse period arguments
    minp, maxp = parsePeriods(data, periodCol, periodKey, periodParam)
    
    #find minimum and maximum row of each account
    minPos={d[acctCol]:i for i, d in enumerate(data) if i==0 or data[i][acctCol]!=data[i-1][acctCol]}
    maxPos={d[acctCol]:i for i, d in enumerate(data) if i==len(data) - 1 or data[i][acctCol]!=data[i+1][acctCol]}
    #return list of values to aggregate for each row
    if periodKey == 'future':
        shifted = [amts[min(maxPos[accts[i]], i+minp) : min(maxPos[accts[i]], i + maxp + 1)] for i in range(len(data))]        
    else:
        shifted = [amts[max(minPos[accts[i]], i-maxp) : max(minPos[accts[i]], i - minp + 1)] for i in range(len(data))]
    return shifted

def getRollingsum(data, 
                  acctCol,
                  periodCol,
                  prodCol,
                  periodKey='between',
                  periodParam=[0,5]):
    priorList = getShiftedList(data, acctCol, periodCol, prodCol, periodKey, periodParam)
    if priorList is None:
        return None
    numP = abs(periodParam[1] - periodParam[0]) + 1
    return [sum(0 if y is None else y for y in inputL) if len(inputL)==numP else np.nan for inputL in priorList]

def getMomentum(data, 
                acctCol,
                periodCol,
                prodCol,
                periodKey='between',
                periodParam=[0,2]):
    priorList1 = getShiftedList(data, acctCol, periodCol, prodCol, periodKey, periodParam)
    periodParam2 = [x + 1 for x in periodParam]
    priorList2 = getShiftedList(data, acctCol, periodCol, prodCol, periodKey, periodParam2)
    if priorList1 is None or priorList2 is None:
        return None
    numerators = [sum([0 if x is None else x for x in inputL]) for inputL in priorList1]
    denominators = [sum([0 if x is None else x for x in inputL]) for inputL in priorList2]
    return [1.0*n/d - 1 if n!=0 and d!=0 else np.nan for n, d in zip(numerators, denominators)]

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
    return [3/(3 + prds) if prds is not None else 0 for prds in prdSincePur]

def getLastPurPeriod(amountL):
    if len(amountL) ==0 or all([amt is None for amt in amountL]):
        return None
    else:
        p = 0
        while amountL.pop() is None:
            p = p + 1
        return p

#apply operator on a list of input for amount/unit use case
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
                returnval = ops[operatorKey](1.0*sum([0 if y is None else y for y in inputList])/len(inputList), compareToVal)
            else: #cast to int to compare as unit
                returnval = ops[operatorKey](int(sum([0 if y is None else y for y in inputList])/len(inputList)), compareToVal)
        elif aggregationKey == 'each':
            returnval = all([ops[operatorKey](0 if y is None else y, compareToVal) for y in inputList])
        elif aggregationKey == 'any':
            returnval = any([ops[operatorKey](0 if y is None else y, compareToVal) for y in inputList])
        return returnval
            
#filter function
#assumption: dataset is already sorted by acctId, periodID
def unifiedFilter(data, 
                  acctCol,
                  periodCol,
                  prodCol,
                  engageKey='engaged',
                  periodKey='ever',
                  periodParam=None,
                  operatorKey='>=',
                  compareToVal=0,
                  aggKey='total',
                  dataset='target'):
    out = None
    minp, maxp = parsePeriods(data, periodCol, periodKey, periodParam)
    numP = maxp - minp + 1
    priorList = getShiftedList(data, acctCol, periodCol, prodCol, periodKey, periodParam)
    #print priorList[10]
    if priorList is not None:
        out = [applyOperator(x, engageKey, operatorKey, aggKey, compareToVal, numP) for x in priorList]
    maxPid = max([x[periodCol] for x in data])
    if out is not None and dataset == 'target':       
        out = [x and data[i][periodCol] == maxPid for i, x in enumerate(out)]
    if out is not None and dataset == 'training':       
        out = [x and data[i][periodCol] < maxPid for i, x in enumerate(out)]
    return out

def generateEvents(data,
                  acctCol,
                  periodCol,
                  prodCol,
                  engageKey='engaged',
                  periodKey='future',
                  periodParam= [1],
                  operatorKey='>=',
                  compareToVal=0,
                  aggKey='total'):
    out = None
    minp, maxp = parsePeriods(data, periodCol, periodKey, periodParam)
    numP = maxp - minp + 1
    futureList = getShiftedList(data, acctCol, periodCol, prodCol, periodKey, periodParam)
    #print priorList[10]
    if futureList is not None:
        out = [applyOperator(x, engageKey, operatorKey, aggKey, compareToVal, numP) for x in futureList]
    maxPid = max([x[periodCol] for x in data])
    if out is not None:
        out = [-1 if data[i][periodCol] == maxPid else int(x) for i, x in enumerate(out)]
    return out                
    
def buildAnalyticPurchaseState(transactionDf, numPeriodsAdded=2):
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
    transactionS = transactionDf[['Account_ID', 'Period_ID', 'Product_ID', 'Amount', 'Quantity']]
    #rename to conform to event table column names
    transactionS.columns = ['LEAccount_ID', 'Period_ID', 'Product_ID', 'Revenue', 'Units']
    #set index, include product_id because pivot doesn't work with multiindex yet
    transactionS = transactionS.set_index(['LEAccount_ID', 'Period_ID', 'Product_ID'])

    #unstack product_id columns, using nan as a filler
    #this is equivalent to keeping account and period ids as index, and pivot on product id
    transactionP = transactionS.unstack('Product_ID', float('nan'))
    #now we need to add intermediate periods even if there is no purchase
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
    apState = transactionP.reset_index(level=[0,1], inplace=False)
    
    # we also need a different format of the same apState table to work with 
    # earlier functions that didn't assume pandas input
    # replace NaN with None because filtering functions assume this
    apStateFill = apState.where((pd.notnull(apState)), None)
    #convert pandas dataframe into a list(index = rowid) of dicts(key=colname)
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

def buildEventTable(apState, apStateDict, acctCol, periodCol, prodCol_tr,
              engageKey_tr, periodKey_tr, periodParam_tr, 
              operatorKey_tr, compareToVal_tr, aggKey_tr,
              prodCol_evt=None,
              engageKey_evt='amount', periodKey_evt='future', periodParam_evt=[1], 
              operatorKey_evt='>', compareToVal_evt=0, aggKey_evt='total'):

    #create training filter, which by default uses the same conditions as target
    #filter, but applied to earlier periods, not the last one    
    trFilter = unifiedFilter(apStateDict, acctCol, periodCol, prodCol_tr,
                             engageKey=engageKey_tr,
                             periodKey=periodKey_tr,
                             periodParam=periodParam_tr,
                             operatorKey=operatorKey_tr,
                             compareToVal=compareToVal_tr,
                             aggKey=aggKey_tr,
                             dataset='training')    
    #generates success events
    #which by default checks target product purchase in the next period
    if prodCol_evt is None:
        prodCol_evt = prodCol_tr
    events = generateEvents( apStateDict, acctCol, periodCol, prodCol_evt,
                             engageKey=engageKey_evt,
                             periodKey=periodKey_evt,
                             periodParam=periodParam_evt,
                             operatorKey=operatorKey_evt,
                             compareToVal=compareToVal_evt,
                             aggKey=aggKey_evt)
    
    apState['Train'] = trFilter
    apState['Target'] = events
    evtbl = apState[apState['Train']==1]

    return evtbl

def buildPreleads(apState, apStateDict, acctCol, periodCol, 
                prodCol_tg,
                engageKey_tg, periodKey_tg, periodParam_tg, 
                operatorKey_tg, compareToVal_tg, aggKey_tg):
    #create target filter
    tgFilter = unifiedFilter(apStateDict, acctCol, periodCol, prodCol_tg,
                             engageKey=engageKey_tg,
                             periodKey=periodKey_tg,
                             periodParam=periodParam_tg,
                             operatorKey=operatorKey_tg,
                             compareToVal=compareToVal_tg,
                             aggKey=aggKey_tg,
                             dataset='target')

    preleads = apState.loc[tgFilter, 'LEAccount_ID']

    return preleads

def comparePreleads(preleads, preleads_pm):
    #comfirm that preleads match
    assert(len(preleads) == preleads_pm.shape[0])
    assert(set(preleads) == set(preleads_pm['Account_ID']))    
    print ('preleads ok')
  
def compareEventTable(evtbl, evtbl_pm):
    #confirm that event table columns match
    #exclude account extension and additional id columns from comparison
    assert(set(evtbl.columns) == set(evtbl_pm.columns))
    print ('column list ok')
    
    #confirm row counts match
    assert(evtbl.shape[0] == evtbl_pm.shape[0])
    print ('row count ok')
    
    #confirm that all missing positions match, and all populated values match
    evtbl.sort_values(by=['LEAccount_ID', 'Period_ID'], inplace=True)
    evtbl_pm.sort_values(by=['LEAccount_ID', 'Period_ID'], inplace=True)
    #set to use the same index
    evtbl.index = evtbl_pm.index

    eps = 0.001
    for col in evtbl.columns:
        print ('Checking: ' + col + ' ...')
        pop1 = evtbl[col].notnull()
        pop2 = evtbl_pm[col].notnull()
        assert (not any(np.logical_xor(pop1, pop2)))
        print ("populated rows are the same")
        diff = evtbl.loc[pop1, col].astype(float) - evtbl_pm.loc[pop2, col].astype(float)
        assert(all(diff < eps))
        print ("values are consistent")

def loadTFData():
    #accountDf = pd.read_csv("./TFDataDeliverable/LEAccount.csv")
    #periodDf = pd.read_csv("./TFDataDeliverable/Period.csv")
    #productDf = pd.read_csv("./TFDataDeliverable/Product.csv")
    transactionDf = pd.read_csv("./TFDataDeliverable/AnalyticTransaction.csv")
    targetDf = pd.read_csv("./TFDataDeliverable/Targets.csv")
    #create analytic purchase state
    apState, apStateDict = buildAnalyticPurchaseState(transactionDf)
    playDefs = pd.read_csv("./TFDataDeliverable/PlayDefinitions.csv")
    playDefs = playDefs.where((pd.notnull(playDefs)), None)
    return transactionDf, targetDf, apState, apStateDict, playDefs

def verifyTFTargets(apState, apStateDict, targetDf, playDefs, playNum=None):
    
    prodMap = {1:36, 2:32}
    egkMap = {"Enaged": "engaged",
             "Not Engaged": "not engaged",
             "Amounts": "amount",
             "Units": "unit"}
    agkMap = {"each": "each",
              "total": "total",
              "average": "average",
              "at least once": "any",
              None: None}
    for i in playDefs.index:
        if playNum is not None and i + 1 != playNum:
            continue
        [name, fltProd, egk, prk, prpram, agk, opr, val] = \
        playDefs.loc[i, ['Play Name', u'FilterProduct', u'EngagedKey', 
                         u'PeriodKey', u'PeriodParam', u'AggKey', u'Operator', 
                         u'CompareToVal']].tolist()
        if egk == 'Units':
            prodCol = "Product_" + str(prodMap[fltProd]) + "_Units"
        else:
            prodCol = "Product_" + str(prodMap[fltProd]) + "_Revenue"
        egk = egkMap[egk]
        prk = prk.lower()
        agk = agkMap[agk]
        prpram = prpram if prpram is None else eval(prpram)
        
        print "======"
        for x in [name, prodCol, egk, prk, prpram, opr, val, agk]:
            print x
        if egk == 'not engaged' and prk=='prior':
            preleads_engaged_within = buildPreleads(apState, apStateDict,
                                     'LEAccount_ID', 'Period_ID', prodCol,
                                     'engaged', 'within', prpram, opr, val, agk)
            preleads_engaged_ever = buildPreleads(apState, apStateDict,
                                     'LEAccount_ID', 'Period_ID', prodCol,
                                     'engaged', 'ever', None, opr, val, agk)
            preleads_engaged_prior = set(preleads_engaged_ever) - set(preleads_engaged_within)
            preleads_all = apState['LEAccount_ID']
            preleads = set(preleads_all) - set(preleads_engaged_prior)
        elif prk == 'prior':
            preleads_within = buildPreleads(apState, apStateDict,
                                     'LEAccount_ID', 'Period_ID', prodCol,
                                     egk, 'within', prpram, opr, val, agk)
            preleads_ever = buildPreleads(apState, apStateDict,
                                     'LEAccount_ID', 'Period_ID', prodCol,
                                     egk, 'ever', None, opr, val, agk)
            preleads = set(preleads_ever) - set(preleads_within)
        else:
            preleads = buildPreleads(apState, apStateDict,
                                     'LEAccount_ID', 'Period_ID', prodCol,
                                     egk, prk, prpram, opr, val, agk)
        preleads_pm = targetDf[targetDf['Play'] == name]
        comparePreleads(preleads, preleads_pm)

def verifyTFPlay13EventTable(apState, apStateDict, targetDf):
    #load in testing dataset for comparison
    evtbl_pm = pd.read_csv("TFTestPlay13ET.csv")
    evtbl_pm_cols = [x for x in evtbl_pm.columns if not x.startswith('Ext')]
    evtbl_pm_cols.remove("AnalyticPurchaseState_ID")
    evtbl_pm_cols.remove("Offset")
    preleads_pm = targetDf[targetDf['Play'] == 'DS_Test_13']

    # build play
    preleads = buildPreleads(apState, apStateDict,
                                'LEAccount_ID', 'Period_ID', 'Product_32_Revenue',
                                 'amount', 'between', [5, 10], '>=', 5000, 'total')
    evtbl = buildEventTable(apState, apStateDict,
                                'LEAccount_ID', 'Period_ID', 'Product_32_Revenue',
                                 'amount', 'between', [5, 10], '>=', 5000, 'total',
                                prodCol_evt = 'Product_36_Revenue')
    
    #confirm consistency between preleads and event tables
    comparePreleads(preleads, preleads_pm)
    compareEventTable(evtbl, evtbl_pm[evtbl_pm_cols])
    print 'Pass all checks!'
    
if __name__ == '__main__':
    pass
    #transactionDf, targetDf, apState, apStateDict, playDefs=loadTFData()
    #verifyTFTargets()
    #verifyTFPlay13EventTable()
    
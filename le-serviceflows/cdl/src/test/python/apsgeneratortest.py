import operator
import pandas as pd
import numpy as np
from apsdataloader import ApsDataLoader 
import apsgenerator

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
    #transactionDf = pd.read_csv("./AnalyticTransaction.csv")
    transactionDf = ApsDataLoader().readDataFrameFromAvro()
    
    targetDf = pd.read_csv("./Targets.csv")
    #create analytic purchase state
    apState, apStateDict = apsgenerator.buildAnalyticPurchaseState(transactionDf)
    playDefs = pd.read_csv("./PlayDefinitions.csv")
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
            preleads_engaged_within = apsgenerator.buildPreleads(apState, apStateDict,
                                     'LEAccount_ID', 'Period_ID', prodCol,
                                     'engaged', 'within', prpram, opr, val, agk)
            preleads_engaged_ever = apsgenerator.buildPreleads(apState, apStateDict,
                                     'LEAccount_ID', 'Period_ID', prodCol,
                                     'engaged', 'ever', None, opr, val, agk)
            preleads_engaged_prior = set(preleads_engaged_ever) - set(preleads_engaged_within)
            preleads_all = apState['LEAccount_ID']
            preleads = set(preleads_all) - set(preleads_engaged_prior)
        elif prk == 'prior':
            preleads_within = apsgenerator.buildPreleads(apState, apStateDict,
                                     'LEAccount_ID', 'Period_ID', prodCol,
                                     egk, 'within', prpram, opr, val, agk)
            preleads_ever = apsgenerator.buildPreleads(apState, apStateDict,
                                     'LEAccount_ID', 'Period_ID', prodCol,
                                     egk, 'ever', None, opr, val, agk)
            preleads = set(preleads_ever) - set(preleads_within)
        else:
            preleads = apsgenerator.buildPreleads(apState, apStateDict,
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
    preleads = apsgenerator.buildPreleads(apState, apStateDict,
                                'LEAccount_ID', 'Period_ID', 'Product_32_Revenue',
                                 'amount', 'between', [5, 10], '>=', 5000, 'total')
    evtbl = apsgenerator.buildEventTable(apState, apStateDict,
                                'LEAccount_ID', 'Period_ID', 'Product_32_Revenue',
                                 'amount', 'between', [5, 10], '>=', 5000, 'total',
                                prodCol_evt = 'Product_36_Revenue')
    
    #confirm consistency between preleads and event tables
    comparePreleads(preleads, preleads_pm)
    compareEventTable(evtbl, evtbl_pm[evtbl_pm_cols])
    print 'Pass all checks!'
    
if __name__ == '__main__':
    transactionDf, targetDf, apState, apStateDict, playDefs=loadTFData()
    verifyTFTargets(apState, apStateDict, targetDf, playDefs)
    verifyTFPlay13EventTable(apState, apStateDict, targetDf)
    
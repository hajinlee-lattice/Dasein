from __future__ import division
import pandas as pd
import numpy as np
from apsgenerator import createAnalyticPurchaseState

from apsdataloader import ApsDataLoader

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

def verifyTFPlay13EventTable():
    #load in testing dataset for comparison
    evtbl_pm = pd.read_csv("TFTestPlay13ET.csv")
    evtbl_pm_cols = [x for x in evtbl_pm.columns if not x.startswith('Ext')]
    evtbl_pm_cols.remove("AnalyticPurchaseState_ID")
    evtbl_pm_cols.remove("Offset")
    evtbl_pm_cols.remove("Train")
    evtbl_pm_cols.remove("Target")
    for col in evtbl_pm.columns:
        if col not in evtbl_pm_cols:
            evtbl_pm.drop(col, axis=1, inplace=True)
    
    #build APS
#     transactionDf = pd.read_csv("./AnalyticTransaction.csv")
    transactionDf = ApsDataLoader().readDataFrameFromAvro()
    apState = createAnalyticPurchaseState(transactionDf)
    
    #select play 13 event table rows
    idDf = evtbl_pm[['LEAccount_ID', 'Period_ID']]
    evtbl = idDf.merge(apState, how='left', on=['LEAccount_ID', 'Period_ID'])

    #confirm consistency between event table and APS
    compareEventTable(evtbl, evtbl_pm)
    print 'Pass all checks!'
    
if __name__ == '__main__':
    verifyTFPlay13EventTable()
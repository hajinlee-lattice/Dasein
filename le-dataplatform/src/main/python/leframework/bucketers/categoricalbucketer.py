from collections import Counter
import math
import csv
from sklearn.metrics.cluster.supervised import entropy

def getSig(sub, overall):
    subCnt, subRate = sub
    oaCnt, oaRate = overall
    r = (subCnt*subRate + oaCnt*oaRate)/(subCnt+oaCnt)
    if r in [0, 1]:
        return 0
    else:
        return (subRate-oaRate)/(math.sqrt(r*(1.0-r)*(1.0/subCnt + 1.0/oaCnt)))
        
def getTuple(xList, eventList, k = None):
    if k is None:
        return (len(eventList), sum(eventList)*1.0/len(eventList))
    ind = [i for (i,x) in enumerate(xList) if x==k]
    count = len(ind)
    if count == 0:
        rate = 0.0
    else:
        rate = sum([eventList[i] for i in ind])*1.0/count    
    return (count, rate)    

def getGroupingResults(xList, eventList):
    
    popCount = len(eventList)
    popRate = sum(eventList)*1.0/len(eventList)
    
    fullValueSet=set(xList)
    output = {}
    for k in fullValueSet:
        count, rate = getTuple(xList, eventList, k)
        sig = getSig((count, rate), (popCount, popRate))
        output.update({k: (count, rate, sig)})
    return output
    
def groupCategoricalVarForModel(xList, eventList, thresholdError = 3, threshBinSize = 0.05):

    popCount = len(eventList)
    popRate = sum(eventList)*1.0/len(eventList)
    
    groupingResults = getGroupingResults(xList, eventList)
    
    featureValBigsize = dict([(x,y) for x,y in groupingResults.items() if y[0] >= popCount*threshBinSize])
    featureValSignificant = dict([(x,y) for x,y in groupingResults.items() if abs(y[2]) >= thresholdError and x not in featureValBigsize.keys()])
    featureValCloseToMean = dict([(x,y) for x,y in groupingResults.items() if abs(y[2]) <= thresholdError*0.5 and x not in featureValBigsize.keys()])
    featureValLeftOver = dict([(x,y) for x,y in groupingResults.items() if x not in featureValBigsize.keys() and x not in featureValSignificant and x not in featureValCloseToMean])
    
    return (featureValBigsize, featureValSignificant, featureValCloseToMean, featureValLeftOver)
    
def mapValues(xList, featureValBigsize, featureValSignificant, featureValClosetomean, featureValLeftOver):

    featureMappedVal = {x:x for x in featureValBigsize.keys()}
    featureMappedVal.update({x:x for x in featureValSignificant.keys()})
    featureMappedVal.update({x:'Close To Mean' for x in featureValClosetomean.keys()})
    featureMappedVal.update({x:'Left Over' for x in featureValLeftOver.keys()})

    return [featureMappedVal[x] for x in xList]

def uncertaintyCoeff(mi, entropy):
    if mi == None or entropy == 0:
        return None
    return mi / entropy    

def calculateMutualInfoBinary(splCnts, posCnts):
    totalLength, totalPositives=sum(splCnts), sum(posCnts)
    rate=[float(totalLength-totalPositives)/totalLength, float(totalPositives)/totalLength]
    def relative_dep(splCnt, posCnt):
         joint_prob=[float(splCnt - posCnt)/totalLength,float(posCnt)/totalLength]
         p_x = float(splCnt)/totalLength
         returnVal=0.0
         for yi in range(2):
            returnVal += joint_prob[yi] * math.log(joint_prob[yi] / (p_x * rate[yi])) if joint_prob[yi] != 0.0 else 0.0
         return returnVal
    mi_components=[relative_dep(splCnt, posCnt) for splCnt, posCnt in zip(splCnts, posCnts)]
    return mi_components
    
def getEntropy(totalCnt, totalEvent):
    probPos = totalEvent*1.0/totalCnt
    return - math.log(probPos)*probPos - math.log(1 - probPos)*(1 - probPos)
    
def getStats(groupingResults):
    
    statsOutput = {}

    keys = groupingResults.keys()
    
    sampleCount = [groupingResults[k][0] for k in keys]    
    eventCount = [groupingResults[k][1] for k in keys]    
        
    miComponents = calculateMutualInfoBinary(sampleCount, eventCount)
    
    entropyVal = getEntropy(sum(sampleCount), sum(eventCount))
    
    statsOutput["uncertaintyCoefficient"] = dict(zip(keys, [uncertaintyCoeff(mi, entropyVal) for mi in miComponents]))
        
    # add global values that don't change by band
    statsOutput["mi"] = sum(miComponents)
    statsOutput["entropyValue"] = entropyVal    
    statsOutput["miComponents"] = dict(zip(keys, miComponents))
    
    return statsOutput
    
def getCatGroupingAndStatsForModel(xList, eventList, thresholdError = 3, threshBinSize = 0.05):
    
    output = {}
    
    groupingResults = {}
    
    featureValBigsize, featureValSignificant, featureValClosetomean, featureValLeftOver = groupCategoricalVarForModel(xList, eventList, thresholdError, threshBinSize)
    
    popRate = sum(eventList)*1.0/len(eventList)
    
    groupingResults.update({k:(v[0], v[0]*v[1], v[1]/popRate) for k, v in featureValSignificant.items()})
    groupingResults.update({k:(v[0], v[0]*v[1], v[1]/popRate) for k, v in featureValBigsize.items()})
    
    if len(featureValClosetomean) <= 1:
        groupingResults.update({k:(v[0], v[0]*v[1], v[1]/popRate) for k, v in featureValClosetomean.items()})
    else:
        values = featureValClosetomean.values()
        count = sum([x[0] for x in values])
        posCount = sum([x[0]*x[1] for x in values])
        lift = posCount/(count*popRate)
        groupingResults.update({'Close To Mean': (count, posCount, lift)})

    if len(featureValLeftOver) <= 1:
        groupingResults.update({k:(v[0], v[0]*v[1], v[1]/popRate) for k, v in featureValLeftOver.items()})
    else:
        values = featureValLeftOver.values()
        count = sum([x[0] for x in values])
        posCount = sum([x[0]*x[1] for x in values])
        lift = posCount/(count*popRate)
        groupingResults.update({'LeftOver': (count, posCount, lift)})
    
    output['groupingResults'] = groupingResults
    
    output.update(getStats(groupingResults))
    
    return output

def getCatGroupingAndStatsForDisplay(groupingResultsModel, thresholdNumValDisplay = 8):
    
    groupedTuple = groupingResultsModel.items()
    
    popRate = sum(x[1] for k, x in groupedTuple)*1.0/sum(x[0] for k, x in groupedTuple)
    
    miscValue = 'Misc.'
    if 'Misc.' in groupingResultsModel:
        miscValue = 'Misc. - LE'
    
    closeToMeanInd = 0
    if 'Close To Mean' in groupingResultsModel:
        closeToMeanInd = 1
        
    leftOverInd = 0    
    if 'LeftOver' in groupingResultsModel:
        leftOverInd = 1
    
    miscInd = (closeToMeanInd + leftOverInd + 1)/2        
    
    nonMiscKeysCnt = len(groupingResultsModel) - (closeToMeanInd + leftOverInd)
    
    if nonMiscKeysCnt + miscInd <= thresholdNumValDisplay:
        nonMiscKeys = set(groupingResultsModel.keys()) - set(['Close To Mean', 'LeftOver'])
    else:
        nonMiscBuckets = [(x, y[0]) for x, y in groupedTuple if x not in ['Close To Mean', 'LeftOver']]
        nonMiscBuckets = sorted(nonMiscBuckets, key = lambda x: x[1], reverse = True)
        nonMiscKeys = set([nonMiscBuckets[i][0] for i in xrange(thresholdNumValDisplay - 1)])
    
    miscKeys = set(groupingResultsModel.keys()) - nonMiscKeys
    
    groupingResultsDisp = {}
    
    miscTuples = [(y[0], y[1]) for x, y in groupedTuple if x in miscKeys]
    
    groupingResultsDisp.update({miscValue: (sum(x[0] for x in miscTuples), sum(x[1] for x in miscTuples), sum(x[1] for x in miscTuples)/(sum(x[0] for x in miscTuples)*popRate))})
    
    groupingResultsDisp.update({x: y for x, y in groupedTuple if x in nonMiscKeys})
    
    output = {}
    
    output['groupingResults'] = groupingResultsDisp
    
    output.update(getStats(groupingResultsDisp))
    
    return output
    
    

    
    
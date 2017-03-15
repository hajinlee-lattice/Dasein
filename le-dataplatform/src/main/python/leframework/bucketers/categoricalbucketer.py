# -*- coding: utf-8 -*-
"""
Created on Tue Oct 13 11:28:46 2015

@author: jhu
"""

import math

def getSig(sub, overall):
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
        return (subRate - oaRate) / (math.sqrt(r * (1.0 - r) * (1.0 / subCnt + 1.0 / oaCnt)))

def getTuple(xList, eventList, k=None):
    """
    input:  xList : feature column
            eventList: event column
            k : special value to look
    output: when k is None, return (overall-population size, overall-population rate); when k is not None, return (sub-population size, sub-population rate) with respect to the value of k
    """
    if k is None:
        return (len(eventList), sum(eventList) * 1.0 / len(eventList))
    ind = [i for (i, x) in enumerate(xList) if x == k]
    count = len(ind)
    if count == 0:
        rate = 0.0
    else:
        rate = sum([eventList[i] for i in ind]) * 1.0 / count
    return (count, rate)

def getGroupingResults(xList, eventList, popCount, popRate):
    """
    input:  xList : feature column
            eventList: event column
    output: (population size, conversion rate, significance) with respect to each distinct value from xList
    """
    fullValueSet = set(xList)
    output = {}
    for k in fullValueSet:
        count, rate = getTuple(xList, eventList, k)
        sig = getSig((count, rate), (popCount, popRate))
        output.update({k: (count, rate, sig)})
    return output

def groupCategoricalVarForModel(xList, eventList, popCount, popRate, thresholdError=3, threshBinSize=0.05):
    '''
    input:  xList : feature column
            eventList: event column
            thresholdError: threshold used to identify feature values that have significant conversion rate than the over all conversion rate
            threshBinSize: threshold used to identify the feature values that have big population size

    output: the set of feature values that:
        1. have significant conversion rate than the over all conversion rate
        2. have big population size
        3. have conversion rate close to the over all conversion rate
        4. don't belong to any of the 3 above sets
    '''

    groupingResults = getGroupingResults(xList, eventList, popCount, popRate)

    featureValBigsize = dict([(x, y) for x, y in groupingResults.items() if y[0] >= popCount * threshBinSize])
    featureValSignificant = dict([(x, y) for x, y in groupingResults.items() if abs(y[2]) >= thresholdError and x not in featureValBigsize.keys()])
    featureValClosetomean = dict([(x, y) for x, y in groupingResults.items() if abs(y[2]) <= thresholdError * 0.5 and x not in featureValBigsize.keys()])
    featureValLeftOver = dict([(x, y) for x, y in groupingResults.items() if x not in featureValBigsize.keys() and x not in featureValSignificant and x not in featureValClosetomean])

    return (featureValBigsize, featureValSignificant, featureValClosetomean, featureValLeftOver)

def uncertaintyCoeff(mi, entropy):
    if mi == None or entropy == 0:
        return None
    return mi / entropy

def calculateMutualInfoBinary(splCnts, posCnts):
    """
    Calculates mutual information from a list of buckets.
    splCnts: sample counts in each bucket
    poscnts: positive events in each bucket
    returns: mutual information in each bucket
    """
    totalLength, totalPositives = sum(splCnts), sum(posCnts)
    rate = [float(totalLength - totalPositives) / totalLength, float(totalPositives) / totalLength]
    def relative_dep(splCnt, posCnt):
        joint_prob = [float(splCnt - posCnt) / totalLength, float(posCnt) / totalLength]
        p_x = float(splCnt) / totalLength
        returnVal = 0.0
        for yi in range(2):
            returnVal += joint_prob[yi] * math.log(joint_prob[yi] / (p_x * rate[yi])) if joint_prob[yi] != 0.0 else 0.0
        return returnVal
    mi_components = [relative_dep(splCnt, posCnt) for splCnt, posCnt in zip(splCnts, posCnts)]
    return mi_components

def getEntropy(totalCnt, totalEvent):
    if totalCnt == totalEvent or totalEvent == 0 or totalCnt == 0:
        return 0.0
    else:
        probPos = totalEvent * 1.0 / totalCnt
        return -math.log(probPos) * probPos - math.log(1 - probPos) * (1 - probPos)

def getStats(groupingResults):
    '''
    (population size, conversion rate, significance) with respect to each distinct value from the feature column, compute the statistics such as mutual information, entropy and  uncertainty coefficient
    '''
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

def getCatGroupingAndStatsForModel(xList, eventList, thresholdError=3, threshBinSize=0.05):
    '''
    input:  xList : feature column
            eventList: event column
            thresholdError: threshold used to identify feature values that have significant conversion rate than the over all conversion rate
            threshBinSize: threshold used to identify the feature values that have big population size
    output: summary for the grouping results used for modeling
    '''
    output = {}

    groupingResults = {}
    popCount = len(eventList)
    popRate = sum(eventList) * 1.0 / len(eventList)
    nullIdx = set([x[0] for x in enumerate(xList) if x[1] is None])
    if len(nullIdx) > 0:
        eventListNull = [eventList[i] for i in xrange(popCount) if i in nullIdx]

        groupingResults.update({None: (len(nullIdx), sum(eventListNull), sum(eventListNull) * 1.0 / (len(nullIdx) * popRate) if popRate > 0.0 else None)})

    xListNotNull = [xList[i] for i in xrange(popCount) if i not in nullIdx]
    eventListNotNull = [eventList[i] for i in xrange(popCount) if i not in nullIdx]

    featureValBigsize, featureValSignificant, featureValClosetomean, featureValLeftOver = groupCategoricalVarForModel(xListNotNull, eventListNotNull, popCount, popRate, thresholdError, threshBinSize)

    groupingResults.update({k:(v[0], v[0] * v[1], v[1] / popRate if popRate > 0.0 else None) for k, v in featureValSignificant.items()})
    groupingResults.update({k:(v[0], v[0] * v[1], v[1] / popRate if popRate > 0.0 else None) for k, v in featureValBigsize.items()})

    if len(featureValClosetomean) <= 1:
        groupingResults.update({k:(v[0], v[0] * v[1], v[1] / popRate if popRate > 0.0 else None) for k, v in featureValClosetomean.items()})
    else:
        values = featureValClosetomean.values()
        count = sum([x[0] for x in values])
        posCount = sum([x[0] * x[1] for x in values])
        lift = posCount / (count * popRate) if popRate > 0.0 else None
        groupingResults.update({'Close To Mean': (count, posCount, lift)})

    if len(featureValLeftOver) <= 1:
        groupingResults.update({k:(v[0], v[0] * v[1], v[1] / popRate if popRate > 0.0 else None) for k, v in featureValLeftOver.items()})
    else:
        values = featureValLeftOver.values()
        count = sum([x[0] for x in values])
        posCount = sum([x[0] * x[1] for x in values])
        lift = posCount / (count * popRate) if popRate > 0.0 else None
        groupingResults.update({'LeftOver': (count, posCount, lift)})

    output['groupingResults'] = groupingResults

    output.update(getStats(groupingResults))

    return output

def getCatGroupingAndStatsForDisplay(groupingResultsModel, thresholdNumValDisplay=8):
    '''
    input:  summary for the grouping results used for modeling
            thresholdNumValDisplay: maximum number of values allowed to be shown on UI
    output: summary for the grouping results used for Displaying on UI
    '''
    groupedTuple = groupingResultsModel.items()

    popRate = sum(x[1] for _, x in groupedTuple) * 1.0 / sum(x[0] for _, x in groupedTuple)

    miscValue = 'Misc.'
    if 'Misc.' in groupingResultsModel:
        miscValue = 'Misc. - LE'

    closeToMeanInd = 0
    if 'Close To Mean' in groupingResultsModel:
        closeToMeanInd = 1

    leftOverInd = 0
    if 'LeftOver' in groupingResultsModel:
        leftOverInd = 1

    miscInd = (closeToMeanInd + leftOverInd + 1) / 2

    nonMiscKeysCnt = len(groupingResultsModel) - (closeToMeanInd + leftOverInd)

    if nonMiscKeysCnt + miscInd <= thresholdNumValDisplay:
        nonMiscKeys = set(groupingResultsModel.keys()) - set(['Close To Mean', 'LeftOver'])
    else:
        nonMiscBuckets = [(x, y[0]) for x, y in groupedTuple if x not in ['Close To Mean', 'LeftOver']]
        nonMiscBuckets = sorted(nonMiscBuckets, key=lambda x: x[1], reverse=True)
        nonMiscKeys = set([nonMiscBuckets[i][0] for i in xrange(thresholdNumValDisplay - 1)])

    miscKeys = set(groupingResultsModel.keys()) - nonMiscKeys

    groupingResultsDisp = {}

    if len(miscKeys) > 0:
        miscTuples = [(y[0], y[1]) for x, y in groupedTuple if x in miscKeys]
        groupingResultsDisp.update({miscValue: (sum(x[0] for x in miscTuples), sum(x[1] for x in miscTuples), sum(x[1] for x in miscTuples) / (sum(x[0] for x in miscTuples) * popRate) if popRate > 0.0 else None)})

    groupingResultsDisp.update({x: y for x, y in groupedTuple if x in nonMiscKeys})

    output = {}

    output['groupingResults'] = groupingResultsDisp

    output.update(getStats(groupingResultsDisp))

    return output

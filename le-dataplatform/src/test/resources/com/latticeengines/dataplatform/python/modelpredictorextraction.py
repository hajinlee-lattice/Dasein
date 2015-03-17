# This version combines the LowerInclusive, UpperExclusive and Bucket Value fields into a single field. 
# For the original output, use v3 version of this script


import os
import sys
import json
import itertools
import operator

reload(sys)
sys.setdefaultencoding('utf-8')

def main(argv):
    
    ModelJSONFilePath = sys.argv[1]
    CSVFilePath = sys.argv[2]
    debugFlag = ''
    if len(sys.argv) > 3:
        debugFlag = sys.argv[3].lower()
    nameArray = []
    sumArray = []
 
    with open (ModelJSONFilePath, "r") as myfile:
        ModelJSON=myfile.read().replace('\n', '')
        
    contentJSON = json.loads(ModelJSON)

    if contentJSON["AverageProbability"] is None:
        print "-------------------------------------------"
        print "No AverageProbability in the model!!"
        print "-------------------------------------------"                
        return 1

    averageProb = contentJSON["AverageProbability"]

    if contentJSON["Summary"] is None:
        print "-------------------------------------------"
        print "No ModelSummary information in the model!!"
        print "-------------------------------------------"                
        return 1
    
    if contentJSON["Summary"]["Predictors"] is None:
        print "-------------------------------------------"
        print "No Predictors information in the model!!"
        print "-------------------------------------------"
        return 1
    
    with open (CSVFilePath, "w") as csvFile:
        if debugFlag == "-debug":
            csvFile.write("Column Name,Attribute Name,Category,FundamentalType,Predictive Power,Attribute Value,Conversion Rate,Lift,Total Leads,Frequency(#),Frequency/Total Leads,ApprovedUsage\n")
        else:        
            csvFile.write("Attribute Name,Category,FundamentalType,Predictive Power,Attribute Value,Conversion Rate,Lift,Total Leads,Frequency(#),Frequency/Total Leads\n")
            
        #This section calculates the total #leads for each predictor. Ideally it should be the same for each predictor, but there seem to be some exceptions
        siddata = sorted(contentJSON["Summary"]["Predictors"], key=operator.itemgetter('Name'))
        sidgroups = itertools.groupby(siddata, operator.itemgetter('Name'))
        
        for key, group in sidgroups: 
            for value in group:
                total = 0
                for i in value['Elements']:
                    total = total + i['Count']
                #print value['Name'], ":\t\t", total
                nameArray.append(value['Name'])
                sumArray.append(total)
        dictArray = dict(zip(nameArray,sumArray))
        #dictArray is a list of dictionaries with attributeName = #Total Leads

        for predictor in contentJSON["Summary"]["Predictors"]:
            if predictor["ApprovedUsage"] in ["None","Model"] and debugFlag != "-debug": continue;
            otherPredictorElements = []
            for predictorElement in predictor["Elements"]:
                if isMergeWithOther(predictor, predictorElement, dictArray):
                    otherPredictorElements.append(predictorElement)
                    continue
                writePredictorElement(debugFlag, len, averageProb, csvFile, dictArray, predictor, predictorElement)
                
            if (len(otherPredictorElements)  > 0):
                mergedPredictorElement = mergePredictorElements(otherPredictorElements, averageProb)
                writePredictorElement(debugFlag, len, averageProb, csvFile, dictArray, predictor, mergedPredictorElement)

def mergePredictorElements(otherElements, averageProb):
    mergedElement = dict()
    mergedElement["Values"] = ["Other"]
    mergedCount = 0
    mergedEvents = 0;
    for element in otherElements:
        leadCount = element["Count"]
        mergedCount += leadCount if leadCount is not None else 0
        if (element["Lift"] is not None and leadCount is not None):
            mergedEvents += element["Lift"] * averageProb * leadCount; 
            
    mergedElement["Count"] = mergedCount
    if (mergedCount != 0) :
        mergedElement["Lift"] = mergedEvents / float(mergedCount) / averageProb
    return mergedElement
    
def isMergeWithOther(predictor, element, dictArray):
    length = 0
    if element["Values"] is not None:
        length = len(element["Values"])
    if (length == 0):
        return False
    if element["Values"][0] == "Other":
        return True
    if ("LowerInclusive" in element or "UpperExclusive" in element):
        return False
    if len(predictor["Elements"]) <=3:
        return False
    if "Count" in element:
        if element["Count"] is None:
            return True
        else:
            freq = element["Count"] / float(dictArray[predictor["Name"]])
            return True if freq < 0.01 else False
    return True
                                                 
def writePredictorElement(debugFlag, len, averageProb, csvFile, dictArray, predictor, predictorElement):
    if debugFlag == "-debug":
        if 'Name' in predictor:
            csvFile.write('"')
            csvFile.write(unicode(predictor["Name"]).replace('"', '""'))
            csvFile.write('"')
        csvFile.write(",")
    if 'DisplayName' in predictor:
        csvFile.write('"')
        csvFile.write(unicode(predictor["DisplayName"]).replace('"', '""'))
        csvFile.write('"')
    csvFile.write(",")
    if 'Category' in predictor:
        csvFile.write('"')
        csvFile.write(unicode(predictor["Category"]).replace('"', '""'))
        csvFile.write('"')
    csvFile.write(",")
    if 'FundamentalType' in predictor:
        csvFile.write('"')
        csvFile.write(unicode(predictor["FundamentalType"]).replace('"', '""'))
        csvFile.write('"')
    csvFile.write(",")
    if 'UncertaintyCoefficient' in predictor:
        csvFile.write('"' + unicode(predictor["UncertaintyCoefficient"]) + '"')
    csvFile.write(",")
    length = 0
    if predictorElement["Values"] is not None:
        length = len(predictorElement["Values"])
    if (length == 0): #This implies that value is of type bucket and not categorical
        if predictorElement["UpperExclusive"] is not None:
            csvFile.write("< ")
            csvFile.write(unicode(predictorElement["UpperExclusive"]))
        else:
            csvFile.write(">= ")
            csvFile.write(unicode(predictorElement["LowerInclusive"]))
        csvFile.write(",")
    elif (predictorElement["Values"])[length - 1] is None:
        csvFile.write("Not Available,")
    else:
        csvFile.write('"[')
        for val in predictorElement["Values"]:
            if val is not None:
                csvFile.write('""')
                newVal = mapBinaryValue(predictor, val)
                csvFile.write(unicode(newVal).replace('"', '""'))
                csvFile.write('""')
                if val != (predictorElement["Values"])[length - 1]:
                    csvFile.write(';')
        
        csvFile.write(']",')
    if 'Lift' in predictorElement:
        csvFile.write(unicode(averageProb * predictorElement["Lift"]))
    csvFile.write(",")
    if 'Lift' in predictorElement:
        csvFile.write(unicode(predictorElement["Lift"]))
    csvFile.write(",")
    if predictor['Name'] in dictArray.keys():
        if dictArray[predictor['Name']] is None:
            csvFile.write("notFound")
        else:
            csvFile.write('"' + str(dictArray[predictor['Name']]) + '"') #write total #leads to csvFile
    csvFile.write(",")
    if 'Count' in predictorElement:
        if predictorElement["Count"] is None:
            csvFile.write("null")
        else:
            csvFile.write(unicode(predictorElement["Count"]))
            csvFile.write(",")
            csvFile.write(unicode(predictorElement["Count"] / float(dictArray[predictor['Name']])))
    csvFile.write(",")
    if debugFlag == "-debug":
        if 'DisplayName' in predictor:
            csvFile.write('"')
            csvFile.write(unicode(predictor["ApprovedUsage"]).replace('"', '""'))
            csvFile.write('"')
    csvFile.write("\n")


def mapBinaryValue(predictor, val):
    if 'FundamentalType' not in predictor:
        return val
    fundamentalType = predictor["FundamentalType"].upper()
    if fundamentalType != "BOOLEAN":
        return val
    upperVal = str(val).upper()
    if upperVal in ["TRUE", "T", "YES", "Y", "C", "D", "1"]:
        return "Yes"
    if upperVal in ["FALSE", "F", "NO", "N", "0"]:
        return "No"
    if upperVal in ["", "NA", "N/A", "-1", "NULL", "NOT AVAILABLE"]:
        return "Not Available"
    return val
    
    
    #print "----------------------------------------------------------------------------"
    #print "successfully extracted predictors information to " + CSVFilePath
    #print "----------------------------------------------------------------------------"
    
if __name__ == "__main__":
    sys.exit(main(sys.argv))
    
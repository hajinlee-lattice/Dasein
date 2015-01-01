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
            for predictorElement in predictor["Elements"]:    
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
                
                if (length == 0):   #This implies that value is of type bucket and not categorical
                    if predictorElement["UpperExclusive"] is not None:
                        csvFile.write("< ")
                        csvFile.write(unicode(predictorElement["UpperExclusive"]))                        
                    else:
                        csvFile.write(">= ")
                        csvFile.write(unicode(predictorElement["LowerInclusive"]))    
                    csvFile.write(",")                    
                else:
                    if (predictorElement["Values"])[length - 1] is None:
                        csvFile.write("Not Available,")                    
                    else:
                        csvFile.write('"[')
                        for val in predictorElement["Values"]:    
                            if val is not None:
                                csvFile.write('""')                           
                                csvFile.write(unicode(val).replace('"', '""'))
                                csvFile.write('""')
                                if val != (predictorElement["Values"])[length - 1]:
                                    csvFile.write(';')                         
                        csvFile.write(']",')
                
                if 'Lift' in predictorElement:
                    csvFile.write(unicode(averageProb*predictorElement["Lift"]))
                csvFile.write(",")
                
                if 'Lift' in predictorElement:
                    csvFile.write(unicode(predictorElement["Lift"]))          
                csvFile.write(",")

                if predictor['Name'] in dictArray.keys():
                    if dictArray[predictor['Name']] is None:
                        csvFile.write("notFound")
                    else:                     
                    #write total #leads to csvFile
                        csvFile.write('"' + str(dictArray[predictor['Name']]) + '"')            
                csvFile.write(",")    
             
                if 'Count' in predictorElement:
                    if predictorElement["Count"] is None:
                        csvFile.write("null")
                    else:
                        csvFile.write(unicode(predictorElement["Count"]))
                        csvFile.write(",")     
                        csvFile.write(unicode(predictorElement["Count"]/float(dictArray[predictor['Name']])))                   
                csvFile.write(",")
                
                if debugFlag == "-debug":
                    if 'DisplayName' in predictor: 
                        csvFile.write('"')                    
                        csvFile.write(unicode(predictor["ApprovedUsage"]).replace('"', '""'))
                        csvFile.write('"')                                                
                csvFile.write("\n")
    
    #print "----------------------------------------------------------------------------"
    #print "successfully extracted predictors information to " + CSVFilePath
    #print "----------------------------------------------------------------------------"
    
if __name__ == "__main__":
    sys.exit(main(sys.argv))
    
# This script is used for comparing the properties in QA cluster and Prod cluster are the same
import sys
import os
import glob
import json

QA_CLUSTER_PROPERTY_DIR = "/conf/env/qacluster/"
PROD_CLUSTER_PROPERTY_DIR = "/conf/env/prodcluster/"
PROPERTY_FILE_SUFFIX = "*.properties"
LINE_SEPERATOR = "\n=============================\n"

def main():
    print "Current directory should be in le-dev: " + os.getcwd()
    os.chdir(os.getcwd()+"/..")
    topLevelDir = os.getcwd()
    print "Change to top level directory: " + topLevelDir
    allDirsAndFiles = os.listdir(os.getcwd())

    for dir in allDirsAndFiles:
        # change directory to each project, and see if they have property files
        if os.path.isdir(dir):
            printCurrentDir(dir)
            qaPropertyExists = os.path.exists(dir + QA_CLUSTER_PROPERTY_DIR)
            prodPropertyExists = os.path.exists(dir + PROD_CLUSTER_PROPERTY_DIR)
            if (not qaPropertyExists) and (not prodPropertyExists):
                print dir + " does not have properties for QA and Prod Cluster.\n"
            # if so, compare the contents of the two property files:
            elif qaPropertyExists and prodPropertyExists:
                comparePropertyFiles(dir)
            # one of the directory does not exist
            elif not qaPropertyExists:
                print QA_CLUSTER_PROPERTY_DIR + " does not exist in directory: " + dir
            else:
                print PROD_CLUSTER_PROPERTY_DIR + " does not exist in directory: " + dir


def printCurrentDir(dir):
    print LINE_SEPERATOR
    print "Current directory is: " + dir
    
def comparePropertyFiles(dir):
    qaPropertyFiles = glob.glob(dir + QA_CLUSTER_PROPERTY_DIR + PROPERTY_FILE_SUFFIX)
    prodPropertyFiles = glob.glob(dir + PROD_CLUSTER_PROPERTY_DIR + PROPERTY_FILE_SUFFIX)
    print qaPropertyFiles
    print prodPropertyFiles
    if len(qaPropertyFiles) != len(prodPropertyFiles):
        print "Directory: " + dir +" does not have identical property files" 
        return
    #open specific property file and compare the contents of them
    for i in range(len(qaPropertyFiles)):
        qaPropertyFileName = qaPropertyFiles[i]
        qaPropertyFile = open(qaPropertyFiles[i])
        commonPropertyName = getPropertyFileName(qaPropertyFileName)
        print "commonPropertyName is " + commonPropertyName
        matchedProdPropertyFileName = findCorrespondingPropertyFile(prodPropertyFiles, commonPropertyName)
        if len(matchedProdPropertyFileName) == 0:
            print "ProdPropertyFiles does not contain " + commonPropertyName 
            return

        prodPropertyFile = open(matchedProdPropertyFileName[0])
        qaPropertyConents = qaPropertyFile.readlines()
        prodPropertyConents = prodPropertyFile.readlines()
        compareContents(qaPropertyConents, prodPropertyConents)
        qaPropertyFile.close()
        prodPropertyFile.close()

def getPropertyFileName(propertyFileName):
    index = propertyFileName.rfind('/')
    return propertyFileName[index+1:len(propertyFileName)]

def findCorrespondingPropertyFile(propertyFiles, commonPropertyName):
    matchedFileName = list(filter((lambda x: getPropertyFileName(x) == commonPropertyName), propertyFiles))
    return matchedFileName

# make sure that everything in qaPropertyConents exists in prodPropertyConents
def compareContents(qaPropertyConents, prodPropertyConents):
    #store two contents into two dictionaries
    qaDictionary = storeContentsIntoDictionary(qaPropertyConents)
    prodDictionary = storeContentsIntoDictionary(prodPropertyConents)
    #compare two dictionaries
    compareTwoDictionaries(qaDictionary, prodDictionary)
    
def storeContentsIntoDictionary(conents):
    dictionary = {}
    for i in range(len(conents)):
        line = conents[i]
        if (not line.startswith( '#' )) and (not line.isspace() ):
            pair = line.split('=', 1)
            if len(pair) == 2:
                dictionary[pair[0]] = pair[1]
            else:
                dictionary[pair[0]] = ''
    return dictionary

def compareTwoDictionaries(qaDictionary, prodDictionary):
    if (qaDictionary == {}) and (prodDictionary == {}):
        return
    qaKeys = set(qaDictionary.keys())
    prodKeys = set(prodDictionary.keys())
    intersectKeys = qaKeys.intersection(prodKeys)
    qaUniqueKeys = qaKeys - intersectKeys
    prodUniqueKeys = prodKeys - intersectKeys
    missingValues = {o : (qaDictionary[o], prodDictionary[o]) for o in intersectKeys if (qaDictionary[o] != prodDictionary[o] and (qaDictionary[o] == '' or prodDictionary[o] == ''))}
    if len(qaUniqueKeys) != 0:
        print "QA property file has the following unique keys:"
        print qaUniqueKeys
    if len(prodUniqueKeys) != 0:
        print "Prod property file has the following unique keys:"
        print prodUniqueKeys
    if len(missingValues) != 0:
        print "Missing values are:"
        print missingValues

if __name__ == "__main__":
    main()


'''
Created on Oct 16, 2014

@author: hliu
'''
import json
from os import listdir
from os.path import isfile, join

def parseJSON(fileName):
    with open(fileName, 'r') as f:
        jsonStr = json.load(f)
        f.close()
    firstDict = fileName[fileName.rfind("/") + 1 : fileName.find(".json")]
    attrDict = {};dictLists = {};dictLists[firstDict] = attrDict
    recursivelyParse(jsonStr, attrDict, dictLists)
    return dictLists
                
def recursivelyParse(jsonStr, attrDict, dictLists):
    for key in jsonStr["properties"].viewkeys():
            attrDict[key] = None;
            if jsonStr["properties"][key]["type"] == "array":
                attrDict[key] = []
            elif jsonStr["properties"][key]["type"] == "object":
                newDict = {}
                dictLists[key] = newDict
                recursivelyParse(jsonStr["properties"][key], newDict, dictLists)

def initDict(path):
    files = [f for f in listdir(path) if isfile(join(path, f))]
    dictList = {}
    for localFile in files: dictList = dict(dictList.items() + parseJSON(join(path, localFile)).items())
    return dictList
    

import json
import csv
import logging
import numpy as np

logging.basicConfig(level = logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='argumentparser')


class ArgumentParser(object):
    """
    This class is responsible for parsing the json file as understood by the 
    LE data platform.
    """
    def __init__(self, metadataFile):
        metadataJson = open(self.stripPath(metadataFile)).read()
        logger.debug("JSON metadata %s" % metadataJson)
        self.metadataSchema = json.loads(metadataJson)
        logger.debug("JSON data schema file name %s" % self.stripPath(self.metadataSchema["schema"]))
        dataSchemaJsonData = open(self.stripPath(self.metadataSchema["schema"])).read()
        logger.debug("JSON data schema %s" % dataSchemaJsonData)
        dataSchema = json.loads(dataSchemaJsonData)
        self.fields = dataSchema["fields"]
        self.features = set(self.metadataSchema["features"])
        self.targets = set(self.metadataSchema["targets"])
        
    def stripPath(self, fileName):
        return fileName[fileName.rfind('/')+1:len(fileName)]
    
    def getField(self, index):
        return self.fields[index]
    
    def convertType(self, cell, fieldType):
        if (fieldType == "int"):
            return int(cell)
        elif (fieldType == "float"):
            return float(cell)
        return cell

    def createList(self, dataFileName):
        '''
          Creates a numpy matrix from the data set in dataFileName. It only creates data in memory for features and targets.
        '''
        csvfile = open(dataFileName, 'Ur')
        tmp = []
        
        # k is the index of the features/target in the resulting data
        k = 0
        # l is the index in the original data set
        l = 0
        included = set()
        self.featureIndex = set()
        for f in self.fields:
            fType = f["type"][0]
            fName = f["name"]
            if fType != 'string' and (fName in self.features or fName in self.targets):
                included.add(l)
                print("Adding " + fName)
                if fName in self.targets:
                    self.targetIndex = k
                if fName in self.features:
                    self.featureIndex.add(k)
                k = k+1
            l = l+1
        
        for row in csv.reader(csvfile, delimiter=','):
            rowlist = []
            if len(row) != len(self.fields):
                msg = "Data-metadata mismatch. Metadata has %s, while data has %s fields." % (len(self.fields), len(row))
                raise Exception(msg)
            for i in included:
                try:
                    rowlist.append(self.convertType(row[i], self.getField(i)["type"][0]))
                except Exception:
                    print("Issue with index " + str(i))
            tmp.append(rowlist)
        return np.array(tmp)
    
    def getSchema(self):
        return self.metadataSchema
    
    def getFeatureTuple(self):
        return tuple(self.featureIndex)
    
    def getTargetIndex(self):
        return self.targetIndex


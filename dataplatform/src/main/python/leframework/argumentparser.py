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
        logger.debug("JSON metadata %s" % (metadataJson))
        self.metadataSchema = json.loads(metadataJson)
        logger.debug("JSON data schema file name %s" % (self.stripPath(self.metadataSchema["schema"])))
        dataSchemaJsonData = open(self.stripPath(self.metadataSchema["schema"])).read()
        logger.debug("JSON data schema %s" % (dataSchemaJsonData))
        dataSchema = json.loads(dataSchemaJsonData)
        self.fields = dataSchema["fields"]
        self.features = set(self.metadataSchema["features"])
        self.targets = set(self.metadataSchema["targets"])
        print(self.features)
        
    def stripPath(self, fileName):
        return fileName[fileName.rfind('/')+1:len(fileName)]
    
    def getField(self, index):
        return self.fields[index]
    
    def convertType(self, cell, fieldType):
        if fieldType == "int":
            return int(cell)
        elif fieldType == "float":
            return float(cell)
        return cell

    def createList(self, dataFileName):
        csvfile = open(dataFileName, 'Ur')
        tmp = []
        for row in csv.reader(csvfile, delimiter=','):
            rowlist = []
            for i in range(len(row)):
                field = self.getField(i)
                fieldType = field["type"][0]
                fieldName = field["name"]
                if (fieldType != 'string' and (fieldName in self.features or fieldName in self.targets)):
                    rowlist.append(self.convertType(row[i], fieldType))
            tmp.append(rowlist)
        return np.array(tmp)
    
    def getSchema(self):
        return self.metadataSchema


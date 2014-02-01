import json
import csv
import logging

logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
        
    def stripPath(self, fileName):
        return fileName[fileName.rfind('/')+1:len(fileName)]
        
    def convertType(self, cell, index):
        fieldType = self.fields[index]["type"][0]
        
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
                rowlist.append(self.convertType(row[i], i))
            tmp.append(rowlist)
        return tmp
    
    def getSchema(self):
        return self.metadataSchema

def createParser(jsonData):
    return ArgumentParser(jsonData)


import csv
import json
import logging
import ConfigParser
from StringIO import StringIO
import fastavro as avro
import pandas as pd

logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='argumentparser')


class ArgumentParser(object):
    
    
    """
    This class is responsible for parsing the json file as understood by the 
    LE data platform.
    """
    def __init__(self, metadataFile, propertyFile = None):
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
        self.keyCols = set(self.metadataSchema["key_columns"])
        self.depivoted = False
        if "depivoted" in self.metadataSchema:
            self.depivoted = self.metadataSchema["depivoted"] == "True"
        self.transformer = None
        
        self.algorithmProperties = self.__parseProperties("algorithm_properties")
        self.provenanceProperties = self.__parseProperties("provenance_properties")
        self.runtimeProperties = self.__parseRuntimeProperties(propertyFile)
        logger.debug("reading runtime properties" + str(self.runtimeProperties))
        
    def __parseProperties(self, name):
        element = {}
        try:
            properties = self.metadataSchema[name]
            element = dict(u.split("=") for u in properties.split(" "))
        except Exception:
            pass
    
        return element
        
    def __parseRuntimeProperties(self, propertyFile):
        if propertyFile is None:
            logger.info("No runtime properties found")
            return None
        
        logger.info("Reading properties file: " + propertyFile)
        properties = '[runtimeconfig]\n' + open(propertyFile, 'r').read()
        properties_fp = StringIO(properties)
        cf = ConfigParser.ConfigParser()
        cf.readfp(properties_fp)
        return dict(cf.items("runtimeconfig"))
        
    def stripPath(self, fileName):
        return fileName[fileName.rfind('/')+1:len(fileName)]
    
    def __getField(self, index):
        return self.fields[index]
    
    def __convertType(self, cell, fieldType):
        if (fieldType == "int"):
            return int(cell)
        elif (fieldType == "float"):
            return float(cell)
        return cell

    def createList(self, dataFileName):
        '''
          Creates a numpy matrix from the data set in dataFileName. It only creates data in memory for features and targets.
        '''
        # k is the index of the features/target in the resulting data
        k = 0
        # l is the index in the original data set
        l = 0
        includedNames = []
        included = []
        self.featureIndex = set()
        self.nameToFeatureIndex = dict()
        self.keyColIndex = set()
        self.stringColNames = set()

        for f in self.fields:
            fType = f["type"][0]
            fName = f["name"]
            if fName in self.features or fName in self.targets or fName in self.keyCols:
                
                logger.info("Adding %s with index %d" % (fName, l))
                includedNames.append(fName)
                included.append(l)
                
                if fName in self.targets:
                    self.targetIndex = k
                if fName in self.features:
                    self.featureIndex.add(k)
                    self.nameToFeatureIndex[fName] = k
                if fName in self.keyCols:
                    self.keyColIndex.add(k)
                if fType == 'string' and fName not in self.targets:
                    self.stringColNames.add(fName)
                k = k+1
            l = l+1
        
        tmp = []

        reader = None 
        filedescriptor = open(dataFileName, 'rb')
        if self.isAvro():
            reader = avro.reader(filedescriptor)
        else:
            reader = csv.reader(filedescriptor)
            
        for row in reader:
            rowlist = []
            if len(row) != len(self.fields):
                msg = "Data-metadata mismatch. Metadata has %s, while data has %s fields." % (len(self.fields), len(row))
                raise Exception(msg)
            for i in included:
                try:  
                    if self.isAvro():
                        value = row[self.__getField(i)["name"]]
                        if i == included[self.targetIndex]:
                            value = float(value)
                        rowlist.append(value)
                    else:
                        rowlist.append(self.__convertType(row[i], self.__getField(i)["type"][0]))
                except Exception as e:
                    logger.error("Issue with index " + str(i))
                    logger.error(str(e))
            tmp.append(rowlist)
        self.__populateSchemaWithMetadata(self.getSchema(), self)
        return pd.DataFrame(tmp, columns=includedNames)
    
    def __populateSchemaWithMetadata(self, schema, parser):
        schema["featureIndex"] = parser.getFeatureTuple()
        schema["features"] = self.metadataSchema["features"]
        schema["targetIndex"] = parser.getTargetIndex()
        schema["keyColIndex"] = parser.getKeyColumns()
        schema["nameToFeatureIndex"] = parser.getNameToFeatureIndex()
        schema["metadata"] = self.stripPath(self.metadataSchema["metadata"])
        schema["targets"] = self.targets
        schema["stringColumns"] = self.stringColNames
     
    def isAvro(self):
        return self.metadataSchema["data_format"] == "avro"
    
    def isDepivoted(self):
        return self.depivoted
    
    def getNameToFeatureIndex(self):
        return self.nameToFeatureIndex
    
    def getSchema(self):
        return self.metadataSchema
    
    def getFeatureTuple(self):
        return tuple(self.featureIndex)
    
    def getTargetIndex(self):
        return self.targetIndex
    
    def getAlgorithmProperties(self):
        return self.algorithmProperties

    def getKeyColumns(self):
        return tuple(self.keyColIndex)
    
    def getStringColumns(self):
        return self.stringColNames

    def getProvenanceProperties(self):
        return self.provenanceProperties

    def getRuntimeProperties(self):
        return self.runtimeProperties

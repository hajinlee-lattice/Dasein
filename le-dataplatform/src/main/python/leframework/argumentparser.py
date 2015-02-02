import ConfigParser
from StringIO import StringIO
import csv
import json
import logging

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
        self.configMetadata = None
        try:
            configMetadataJson = open(self.stripPath(self.metadataSchema["config_metadata"])).read()
            self.configMetadata = json.loads(configMetadataJson)
            logger.debug("JSON config metadata schema %s" % configMetadataJson)
        except:
            logger.warn("Config metadata does not exist!")

        self.fields = dataSchema["fields"]
        self.features = self.metadataSchema["features"]
        (self.target, self.readouts) = self.extractTargets()
        self.nonScoringTargets = self.readouts
        self.keys = self.metadataSchema["key_columns"]
        self.depivoted = False
        if "depivoted" in self.metadataSchema:
            self.depivoted = self.metadataSchema["depivoted"] == "True"
        self.transformer = None

        self.algorithmProperties = self.__parseProperties("algorithm_properties")
        self.provenanceProperties = self.__parseProperties("provenance_properties")
        self.runtimeProperties = self.__parseRuntimeProperties(propertyFile)
        logger.debug("Reading runtime properties %s" % str(self.runtimeProperties))
        self.numOfSkippedRow = 0
        self.highUCThreshold = 0.2

    def extractTargets(self):
        specifiedTargets = self.metadataSchema["targets"]

        def logWarning(column, columnType):
            message = "The following " + columnType + " column does not exist: \"" + column + "\"."
            possibleIssue = "The ModelTargets DL parameter may be formatted incorrectly."
            logger.warn(message + " " + possibleIssue)

        fields = set([e["name"] for e in self.fields])
        def columnExists(column): return column in fields

        eventKey = "Event".lower()
        readoutKey = "Readouts".lower()

        target = None; readouts = []
        for sTarget in specifiedTargets:
            pair = sTarget.split(":")
            if len(pair) == 2:
                key = pair[0].strip().lower()
                value = pair[1].strip()
                if key == eventKey:
                    if columnExists(value): target = value
                    else: logWarning(value, "target")
                elif key == readoutKey:
                    for value in [e.strip() for e in value.split("|")]:
                        if columnExists(value): readouts.append(value)
                        else: logWarning(value, "readout")
                else: logWarning(value, "unspecified")
            # Legacy
            elif len(pair) == 1:
                value = pair[0]
                if columnExists(value): target = value
                else: logWarning(value, "target")

        return target, readouts

    def __parseProperties(self, name):
        element = {}
        try:
            properties = self.metadataSchema[name].strip()
            element = dict(u.split("=") for u in properties.split(" "))
        except Exception, e:
            logger.error(str(e))

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
        if cell is None or len(str(cell)) == 0:
            return None
        elif (fieldType == "int"):
            return int(cell)
        elif (fieldType == "float"):
            return float(cell)
        return cell

    def createList(self, dataFileName):
        '''
          Creates a pandas dataframe from the data set in dataFileName. It only creates data in memory for features and targets.
        '''
        includedNames = []
        included = []
        nonScoringIncludedNames = []
        nonScoringIncluded = []
        self.stringColNames = set()

        scoringColumns = set(self.features) | set([self.target]) | set(self.keys)
        nonScoringColumns = set(self.readouts)

        for i, f in enumerate(self.fields):
            fType = f["type"][0]
            fName = f["name"]
            if fName in scoringColumns:
                logger.info("Adding %s with index %d" % (fName, i))
                includedNames.append(fName)
                included.append(i)
                if (fType == 'string' or fType == 'bytes' or fType == 'boolean') and fName != self.target:
                    self.stringColNames.add(fName)
            elif fName in nonScoringColumns:
                nonScoringIncludedNames.append(fName)
                nonScoringIncluded.append(i)

        tmpData = []; nonScoringData = []

        reader = None 
        filedescriptor = open(dataFileName, 'rb')
        if self.isAvro():
            reader = avro.reader(filedescriptor)
        else:
            reader = csv.reader(filedescriptor)

        numberOfNullTarget = 0
        for row in reader:
            rowlist = []; nonScoringlist = []
            if len(row) != len(self.fields):
                msg = "Data-metadata mismatch. Metadata has %s, while data has %s fields." % (len(self.fields), len(row))
                raise Exception(msg)

            if self.isAvro():
                targetName = self.target
                if row[targetName] is None:
                    numberOfNullTarget += 1
                    continue
                else:
                    row[targetName] = float(row[targetName])
                rowlist = [str(row[name]) if name in self.stringColNames and row[name] is not None else row[name] for name in includedNames]
                nonScoringlist = [row[name] for name in nonScoringIncludedNames]
            else:
                # CSV format
                rowlist = [float(row[i]) if self.__getField(i)["name"] == self.target else self.__convertType(row[i], self.__getField(i)["type"][0]) for i in included]
                nonScoringlist = [self.__convertType(row[i], self.__getField(i)["type"][0]) for i in nonScoringIncluded]

            tmpData.append(rowlist)
            nonScoringData.append(nonScoringlist)

        if numberOfNullTarget > 0:
            self.numOfSkippedRow += numberOfNullTarget
            logger.warn("Because target value is None, skipping %s rows." % str(numberOfNullTarget))

        # Defense (Filter Scoring Columns)
        self.nonScoringTargets = filter(lambda e: e not in includedNames, self.nonScoringTargets)
        self.readouts = filter(lambda e: e not in includedNames, self.readouts)

        self.__populateSchemaWithMetadata(self.getSchema(), self)

        scoringFrame = pd.DataFrame(tmpData, columns=includedNames)
        nonScoringFrame = pd.DataFrame(nonScoringData, columns=nonScoringIncludedNames)

        return scoringFrame, nonScoringFrame

    def __populateSchemaWithMetadata(self, schema, parser):
        schema["features"] = self.metadataSchema["features"]
        schema["keys"] = parser.getKeys()
        if "data_profile" in self.metadataSchema:
            schema["data_profile"] = self.stripPath(self.metadataSchema["data_profile"]) 
        schema["config_metadata"] = parser.getConfigMetadata()
        schema["target"] = self.target
        schema["nonScoringTargets"] = self.nonScoringTargets
        schema["readouts"] = self.readouts
        schema["stringColumns"] = self.stringColNames
        schema["fields"] = { k['name']:k['type'][0] for k in self.fields }

    def isAvro(self):
        return self.metadataSchema["data_format"] == "avro"

    def isDepivoted(self):
        return self.depivoted

    def getSchema(self):
        return self.metadataSchema

    def getAlgorithmProperties(self):
        return self.algorithmProperties

    def getKeys(self):
        return self.keys

    def getStringColumns(self):
        return self.stringColNames

    def getProvenanceProperties(self):
        return self.provenanceProperties

    def getRuntimeProperties(self):
        return self.runtimeProperties

    def getConfigMetadata(self):
        return self.configMetadata

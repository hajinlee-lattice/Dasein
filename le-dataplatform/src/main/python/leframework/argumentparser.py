import ConfigParser
from StringIO import StringIO
import csv
import json
import logging
import sys

import fastavro as avro
from leframework.util.reservedfieldutil import ReservedFieldUtil
import pandas as pd


reload(sys)
sys.setdefaultencoding('utf-8')

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
        (self.target, self.readouts, self.samples, self.revenueColumn, self.templateVersion) = self.extractTargets()
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
        companyKey = "Company".lower()
        lastNameKey = "LastName".lower()
        firstNameKey = "FirstName".lower()
        spamIndicatorKey = "SpamIndicator".lower()
        revenueKey = "Revenue".lower()
        templateVersionKey = "Template_Version".lower()

        templateVersion = None; revenueColumn = None
        target = None; readouts = []; samples = dict()
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
                elif key == companyKey:
                    if columnExists(value): samples[companyKey] = value
                    else: logWarning(value, "company")
                elif key == lastNameKey:
                    if columnExists(value): samples[lastNameKey] = value
                    else: logWarning(value, "lastname")
                elif key == firstNameKey:
                    if columnExists(value): samples[firstNameKey] = value
                    else: logWarning(value, "firstname")
                elif key == spamIndicatorKey:
                    if columnExists(value): samples[spamIndicatorKey] = value
                    else: logWarning(value, "spamindicator")
                elif key == revenueKey:
                    if columnExists(value): revenueColumn = value
                    else: logWarning(value, "revenueColumn")
                elif key == templateVersionKey:
                    templateVersion = value
                else: logWarning(value, "unspecified")
            # Legacy
            elif len(pair) == 1:
                value = pair[0]
                if columnExists(value): target = value
                else: logWarning(value, "target")

        return target, readouts, samples, revenueColumn, templateVersion

    def __parseProperties(self, name):
        element = {}
        try:
            properties = self.metadataSchema[name].strip()
            element = dict(u.split("=") for u in properties.split(" "))
        except Exception, e:
            logger.error(str(e))

        return element

    def __parseRuntimeProperties(self, propertyFile):
        if propertyFile is None or propertyFile  == "None":
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

    def createList(self, dataFileName, postProcessClf=True):
        '''
          Creates a pandas dataframe from the data set in dataFileName. It only creates data in memory for features and targets.
        '''
        includedNames = []
        included = []
        self.stringColNames = set()

        scoringColumns = set(self.features) | set([self.target]) | set(self.keys) 
        scoringColumns |= set([self.revenueColumn]) if self.revenueColumn != None else set()
        nonScoringColumns = set(self.readouts) | set(self.samples.values())

        if postProcessClf:
            specifiedColumns = scoringColumns | nonScoringColumns
            (self.reserved, reservedFields, reservedFieldDefaultValues) = ReservedFieldUtil.configureReservedFields()
        else:
            specifiedColumns = scoringColumns
            self.reserved = None

        for i, f in enumerate(self.fields):
            fType = f["type"][0]
            fName = f["name"]
            if fName in specifiedColumns:
                logger.info("Adding %s with index %d" % (fName, i))
                includedNames.append(fName)
                included.append(i)
                if (fType == 'string' or fType == 'bytes') and fName != self.target:
                    self.stringColNames.add(fName)

        tmpData = []

        reader = None 
        filedescriptor = open(dataFileName, 'rb')
        if self.isAvro():
            reader = avro.reader(filedescriptor)
        else:
            reader = csv.reader(filedescriptor)
        
        numberOfNullTarget = 0
        for row in reader:
            rowlist = list(reservedFieldDefaultValues) if postProcessClf else []
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
                rowlist += [None if row[name] is None else str(row[name]) if name in self.stringColNames else float(row[name]) for name in includedNames]
            else:
                # CSV format
                rowlist += [float(row[i]) if self.__getField(i)["name"] == self.target else self.__convertType(row[i], self.__getField(i)["type"][0]) for i in included]

            tmpData.append(rowlist)

        if numberOfNullTarget > 0:
            self.numOfSkippedRow += numberOfNullTarget
            logger.warn("Because target value is None, skipping %s rows." % str(numberOfNullTarget))

        # Defense (Filter Scoring Columns)
        self.readouts = filter(lambda e: e not in scoringColumns, self.readouts)

        self.__populateSchemaWithMetadata(self.getSchema(), self)

        dataFrameColumns = reservedFields + includedNames if postProcessClf else includedNames
        return pd.DataFrame(tmpData, columns=dataFrameColumns)

    def __populateSchemaWithMetadata(self, schema, parser):
        schema["features"] = self.metadataSchema["features"]
        schema["keys"] = parser.getKeys()
        if "data_profile" in self.metadataSchema:
            if schema["data_profile"].rfind('/') > 0 : schema["diagnostics_path"] = schema["data_profile"][0:schema["data_profile"].rfind('/')+1]
            schema["data_profile"] = self.stripPath(self.metadataSchema["data_profile"])
        schema["config_metadata"] = parser.getConfigMetadata()
        schema["target"] = self.target
        schema["reserved"] = self.reserved
        schema["readouts"] = self.readouts
        schema["samples"] = self.samples
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

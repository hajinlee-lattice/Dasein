import logging
import os
import pwd
import sys
import traceback
import json
import uuid
import time
from pandas.core.frame import DataFrame
from urlparse import urlparse
from hdfsmodelreviewreport import ModelingEnvironment
from hdfsmodelreviewreport import hdfsModelReviewReport
from kazoo.client import KazooClient
import thread


logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='datasciencelauncher')
logger.info("passed logging")

def bytes_to_int(bytes):
    return int(bytes[0])

def int_to_bytes(value):
    return str(value)

class PropertiesFileReader(object):
    def __init__(self, propertiesFilePath):
        self.propertiesFilePath = propertiesFilePath
        self.properties = self.readPropertiesFile()

    def readPropertiesFile(self):
        toReturn = {}
        try:
            for line in open(self.propertiesFilePath):
                toParse = line.strip()
                if toParse == "":
                    continue

                if line.startswith("#"):
                    continue

                entries = line.split("=")
                if len(entries) < 2:
                    continue

                toReturn[entries[0].strip()] = entries[1].strip()

        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.error( ''.join('!! ' + line for line in lines))

        return toReturn

    def getProperty(self, key):
        if key is None or key == "":
            return None

        toReturn = self.properties.get(key, None)
        if toReturn is not None:
            if toReturn.startswith("$"):
                variable = toReturn.replace("[{,}]", "")
                return os.environ.get(variable, "")

        return toReturn

    def getZookeeperConnectionString(self):
        return self.getProperty("camille.zk.connectionString")

    def getWebHDFSString(self):
        toReturn = self.getProperty("hadoop.fs.web.webhdfs")
        if toReturn is None:
            return None

        return toReturn.replace("webhdfs://","")

    def getHDFSServer(self):
        toReturn = self.getWebHDFSString()
        if toReturn is None:
            return None

        return toReturn.split(":")[0]

    def getHDFSPort(self):
        toReturn = self.getWebHDFSString()
        if toReturn is None:
            return None

        return toReturn.split(":")[1]

class DataScienceLauncher(object):
    def __init__(self, clientID, zookeeperConnectionString, hdfsServer, hdfsPort, hdfsUser):
        logger.info("Initialized Data Science Launcher")
        self.zookeeperConnectionString = zookeeperConnectionString
        self.hdfsServer = hdfsServer
        self.hdfsPort = hdfsPort
        self.hdfsUser = hdfsUser
        self.numRounds = -1
        self.pathToMonitor = "/Workflow/Modeling/DataScience/Requests"
        self.failedRequestsFolder = "/Workflow/Modeling/DataScience/Failures"
        self.executorIDAttr = "ExecutorID"
        self.tryNumAttribute = "TryNum"
        self.dataAttribute = "Data"
        self.executorID = clientID
        self.configDir =  "/app/dataplatform/config/datascience"

    def strip_trailing(self, lhs):
        if(lhs.endswith('/')):
            return str(lhs)[0:len(str(lhs)) - 1]

        return lhs

    def strip_leading(self, rhs):
        if(str(rhs).startswith("/")):
            return str(rhs)[1: len(str)]

        return rhs

    def compose_paths(self, lhs, rhs):
        if(lhs == ""):
            return rhs

        if(rhs == ""):
            return lhs

        return self.strip_trailing(lhs) + "/" + self.strip_leading(rhs)


    def find_one_open(self, zk):
        try:
            if(zk.exists(self.pathToMonitor)):
                for childNode in zk.get_children(self.pathToMonitor):
                    try:
                        child = self.compose_paths(self.pathToMonitor, childNode)
                        if((not zk.exists(child)) or
                               (not zk.exists(self.compose_paths(child, self.tryNumAttribute))) or
                               zk.exists(self.compose_paths(child, self.executorIDAttr))):
                            continue

                        if(bytes_to_int(zk.get(self.compose_paths(child, self.tryNumAttribute))) >= 3):
                            continue

                        return child
                    except:
                        continue

            return ""
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.error( ''.join('!! ' + line for line in lines))
            return ""

    def remove_hdfs_info(self, inputPath):
        strVersion = str(inputPath)
        if(strVersion.startswith("hdfs://")):
            toReturn = ""
            parts = strVersion.split("/")
            parts = parts[3:len(parts)]
            for part in parts:
                if(part != ""):
                    toReturn = toReturn + "/" + part

        return toReturn

    def execute_one(self, zk, child):
        executorNodePath = self.compose_paths(child, self.executorIDAttr)
        executorNode = zk.create(executorNodePath, self.executorID, None, True )
        tryNum = bytes_to_int(zk.get(self.compose_paths(child, self.tryNumAttribute)))
        lookupData = ""
        try:
            lookupData = zk.get(child) [0]
            jsonData = json.loads(lookupData)
            modelDirectory = self.remove_hdfs_info(jsonData['modelDirectory'])
            extractsDirectory = self.remove_hdfs_info(jsonData['extractsDirectory'])
            modelID = jsonData['model_id']

            hdfsModelReviewReport(modelID, modelDirectory, extractsDirectory, self.configDir)

            zk.delete(child, recursive= True)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.error( ''.join('!! ' + line for line in lines))
            if(tryNum >= 2):
                childName = child.split('/')[-1]
                failureName = self.compose_paths(self.failedRequestsFolder, childName)
                zk.create(failureName, lookupData)
                zk.delete(child, recursive=True)
            else:
                zk.set(self.compose_paths(child, self.tryNumAttribute), int_to_bytes(tryNum + 1))

    def process_one(self):
        hostsString = self.zookeeperConnectionString
        zkClient = KazooClient(hosts=hostsString)
        try:
            logger.info("Checking for Zookeeper Datascience Request")
            zkClient.start()
            toProcess = self.find_one_open(zkClient)
            if(toProcess != ""):
                logger.info("Found a Zookeeper Datascience Request")
                self.execute_one(zkClient, toProcess)

        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.error( ''.join('!! ' + line for line in lines))
            logger.error("Could not process a request")

        finally:
            zkClient.stop()

    def main(self, numRepeats):
        logger.info("Entering Zookeeper DataScience Request Loop")
        while(numRepeats != 0):
            try:
                numRepeats = numRepeats - 1
                self.process_one()

            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logger.error( ''.join('!! ' + line for line in lines))

            time.sleep(10)

if __name__ == "__main__":
    """
    Transform the inputs into a Zookeeper System to follow
    
    Arguments:
    sys.argv[1]: hdfsUser
    sys.argv[2]: propertiesFilePath=
    
     Arguments:
    sys.argv[1] -- clientID
    sys.argv[2] -- zookeeperConnectionString
    sys.argv[3] -- hdfsServer
    sys.argv[4] -- hdfsPort
    sys.argv[5] -- hdfsUser
    """
    try:
        argNum = 0
        for arg in sys.argv:
            logger.info("Call Parameter " + str(argNum) + ": " + arg)

        if len(sys.argv) < 3:
            propertiesFile = "/latticeengines.properties"
        else:
            propertiesFile = sys.argv[2]

        if len(sys.argv) < 2:
            hdfsUser = "bross"
        else:
            hdfsUser = sys.argv[1]

        properties = PropertiesFileReader(propertiesFile)

        runID = str(uuid.uuid4())

        configDir = "/app/dataplatform/config/datascience"
        ModelingEnvironment.initialize(properties.getHDFSServer(),
                                       properties.getHDFSPort(),
                                       hdfsUser,
                                       runID)

        # Will eventually pass this in to determine whether to loop indefinitely
        numRepeats = -1
        dsl = DataScienceLauncher(runID,
                                  properties.getZookeeperConnectionString(),
                                  properties.getHDFSServer(),
                                  properties.getHDFSPort(),
                                  hdfsUser)

        dsl.main(numRepeats)
    except:
        logger.error("Error Encountered, about to log")

        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.error( ''.join('!! ' + line for line in lines))




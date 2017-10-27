import logging
import os
import pandas as pd
import fastavro as avro
import json
import glob
import pwd
import shutil
from pandas import DataFrame
from urlparse import urlparse
from leframework.webhdfs import WebHDFS

logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='ApsDataLoader')

class ApsDataLoader(object):
    def __init__(self):
        logger.info("Start data loader.")
        if not os.environ.has_key("StepflowConfig"):
            logger.info("There's no StepflowConfig specified!")
            return
        if not os.environ.has_key("SHDP_HD_FSWEB"):
            logger.info("There's no SHDP_HD_FSWEB specified!")
            return
          
        self.flowConfig = self.stepFlowConfig = os.environ['StepflowConfig']
        logger.info('flowConfig=%s' % self.flowConfig)
        flowJson = json.loads(self.flowConfig)
        self.inputPaths = flowJson['inputPaths'] if 'inputPaths' in flowJson else None
        self.outputPath = flowJson['outputPath'] if 'outputPath' in flowJson else None
        
        webHdfsHostPort = urlparse(os.environ['SHDP_HD_FSWEB'])
        logger.info("Web hdfs host=" + str(webHdfsHostPort.hostname) + " port=" + str(webHdfsHostPort.port))
        self.hdfs = WebHDFS(webHdfsHostPort.hostname, webHdfsHostPort.port, pwd.getpwuid(os.getuid())[0])
        
    def downloadToLocal(self, localDir='./input'):
        logger.info("Start to downloadToLocal data.")
        newInputPaths = []
        for inputPath in self.inputPaths:
            if inputPath.endswith("*.avro"):
                filePath, fileName = os.path.split(inputPath)
                files = self.hdfs.listdir(filePath)
                newInputPaths = newInputPaths + [ filePath + "/" + file for file in files if file.endswith(".avro")]
            else:
                newInputPaths.append(inputPath)
        i = 0
        for inputPath in newInputPaths:
            localFile = "%s/%s%s.avro" % (localDir, 'part-0000', str(i))
            self.hdfs.copyToLocal(inputPath, localFile)
            logger.info("Downloaded file from remote=%s to local=%s." % (inputPath, localFile))
            i += 1
        logger.info("Finished downloadToLocal data.")
    
    
    def uploadFromLocal(self, localDir='./output'):
        logger.info("Start to uploadFromLocal data.")
        outputFiles = glob.glob("%s/*.avro" % localDir)
        for outputFile in outputFiles:
            logger.info("Uploading file from local=%s to remote=%s" % (outputFile, self.outputPath))
            filePath, fileName = os.path.split(outputFile)
            self.hdfs.copyFromLocal(outputFile, self.outputPath + "/" + fileName)
            logger.info("Uploaded file from local=%s to remote=%s" % (outputFile, self.outputPath))
        logger.info("Finished uploadFromLocal data.")

    def readDataFrameFromAvro(self, localDir='./input'):
        logger.info("Start to read Dataframe from avro file.")
        inputFiles = glob.glob("%s/*.avro" % localDir)
        dfs = []
        for inputFile in inputFiles:
            with open(inputFile, "rb") as fp:
                reader = avro.reader(fp)
                records = [r for r in reader]
                df = pd.DataFrame.from_records(records)
                dfs.append(df)
        logger.info("Finished reading Dataframe from avro file.")
        return pd.concat(dfs)
        
    def writeDataFrameToAvro(self, dataFrame, localDir='./output'):
        logger.info("Start to write Dataframe to avro file.")
        schemaStr = self._getSchema(dataFrame)
        schema = json.loads(schemaStr)
        with open("%s/part-00000.avro" % localDir, "wb") as fp:
            records = []
            for record in dataFrame.itertuples(index=False, name=None):
               datum = {schema['fields'][i]['name']:(record[i] if pd.notnull(record[i]) else None) for i in range(len(schema['fields']))}
               records.append(datum)
            avro.writer(fp, schema, records)
        logger.info("Finished writing Dataframe to avro file in directory=%s" % localDir)

    def _getSchema(self, dataFrame):
        schema = """{
        "name": "APS",
        "type": "record",
        "fields": [
            %s
        ]
        }"""
        columns = ['{"name": "%s", "type": ["%s", "null"]}' % (c, self._getType(dataFrame[c].dtype)) for c in dataFrame.columns]
        return schema % ",".join(columns)
    
    def _getType(self, colType):
        dataType = str(colType)
        if 'int32' in dataType:
            return 'int'
        if 'int' in dataType:
            return 'long'
        if 'float32' in dataType:
            return 'float'
        if 'float' in dataType:
            return 'double'
        return 'string'
     
if __name__ == '__main__':
    shutil.rmtree("./input", ignore_errors=True)
    os.mkdir("./input")
    shutil.rmtree("./output", ignore_errors=True)
    os.mkdir("./output")
    
    loader = ApsDataLoader()
    loader.downloadToLocal()
    df = loader.readDataFrameFromAvro()
    loader.writeDataFrameToAvro(df)
    print df.shape
    assert df.shape == (873967, 24)
    csvdf = pd.read_csv("./AnalyticTransaction.csv")
    assert(df.shape == csvdf.shape)
    

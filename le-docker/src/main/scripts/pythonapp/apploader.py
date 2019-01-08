import logging
import os
import json
import glob
import pwd
import shutil
from urlparse import urlparse
from webhdfs import WebHDFS

import tarfile
import zipfile

logging.basicConfig(level=logging.DEBUG, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='AppLoader')

class AppLoader(object):
    def __init__(self):
        logger.info("Start app loader.")
        if not os.environ.has_key("PYTHON_APP_CONFIG"):
            logger.info("There's no PYTHON_APP_CONFIG specified!")
            return
        if not os.environ.has_key("PYTHON_APP_LAUNCH"):
            logger.info("There's no PYTHON_APP_LAUNCH specified!")
            return
        if not os.environ.has_key("SHDP_HD_FSWEB"):
            logger.info("There's no SHDP_HD_FSWEB specified!")
          
        self.appConfig = os.environ['PYTHON_APP_CONFIG']
        logger.info('PYTHON_APP_CONFIG=%s' % self.appConfig)
        appJson = json.loads(self.appConfig)
        
        self.inputPaths = appJson['inputPaths'] if 'inputPaths' in appJson else None
        self.outputPath = appJson['outputPath'] if 'outputPath' in appJson else None
        
        self.appLauncher = os.environ['PYTHON_APP_LAUNCH']
        args = os.environ['PYTHON_APP_ARGS']
        self.appArgs = None
        if (args is not None):
            self.appArgs = args.split();
            self.appArgs = [self.appLauncher] + self.appArgs
        self.webHdfsHostPort = urlparse(os.environ['SHDP_HD_FSWEB'])
        logger.info("Web hdfs host=" + str(self.webHdfsHostPort.hostname) + " port=" + str(self.webHdfsHostPort.port))
        self.hdfs = WebHDFS(self.webHdfsHostPort.hostname, self.webHdfsHostPort.port, pwd.getpwuid(os.getuid())[0])
        
    def extract_file(self, path, to_directory='.'):
        if path.endswith('.zip'):
            opener, mode = zipfile.ZipFile, 'r'
        elif path.endswith('.tar.gz') or path.endswith('.tgz'):
            opener, mode = tarfile.open, 'r:gz'
        elif path.endswith('.tar.bz2') or path.endswith('.tbz'):
            opener, mode = tarfile.open, 'r:bz2'
        else: 
            raise ValueError, "Could not extract `%s` as no appropriate extractor is found" % path
        file = opener(path, mode)
        try: file.extractall(to_directory)
        finally: file.close()
            
    def downloadToLocal(self, curDir='.'):
        logger.info("Start to downloadToLocal data.")
        newInputPaths = []
        for inputPath in self.inputPaths:
            if inputPath.endswith("*"):
                filePath, fileName = os.path.split(inputPath)
                files = self.hdfs.listdir(filePath)
                newInputPaths = newInputPaths + [ filePath + "/" + file for file in files]
            else:
                newInputPaths.append(inputPath)
        self.libPaths = [curDir]
        for inputPath in newInputPaths:
            filePath, fileName = os.path.split(inputPath)
            localFile = curDir + "/" + fileName
            logger.info("Downloading file from remote=%s to local=%s." % (inputPath, localFile))
            if fileName.endswith(".tar.gz") or fileName.endswith(".zip") or fileName.endswith(".tar.bz2"):
                origFile = curDir + "/" + "orig_" + fileName
                if os.path.exists(origFile):
                    continue
                self.hdfs.copyToLocal(inputPath, origFile)
                os.makedirs(localFile) 
                self.extract_file(origFile, localFile)
                self.libPaths.append(localFile)
            else:
                self.hdfs.copyToLocal(inputPath, localFile)
            logger.info("Downloaded file from remote=%s to local=%s." % (inputPath, localFile))
        logger.info("Finished downloadToLocal data.")
    
    

import logging
import os
import pwd
from urlparse import urlparse

from leframework.codestyle import overrides
from leframework.model.state import State
from leframework.webhdfs import WebHDFS


class PmmlCopyFile(State):
    
    def __init__(self):
        State.__init__(self, "PmmlCopyFile")
        self.logger = logging.getLogger(name = 'pmmlcopyfile')

    @overrides(State)
    def execute(self):
        provenanceProperties = self.mediator.provenanceProperties
        if "PMML_File" in provenanceProperties:
            webHdfsHostPort = urlparse(os.environ['SHDP_HD_FSWEB'])
            hdfs = WebHDFS(webHdfsHostPort.hostname, webHdfsHostPort.port, pwd.getpwuid(os.getuid())[0])
            pmmlHdfsPath = provenanceProperties["PMML_File"]
            
            try:
                hdfs.copyToLocal(pmmlHdfsPath, self.mediator.modelLocalDir + "/rfpmml.xml")
            except:
                self.logger.error("Cannot copy PMML file to local.")



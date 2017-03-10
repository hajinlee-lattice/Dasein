
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import json

from .entitybase import EntityBase
from .pipelineresource import PipelineResource

class PipelineStep(EntityBase):

    @classmethod
    def createFromConfig(cls, config):
        pstep = PipelineStep(config['Name'])
        pstep.setKeyWhenSortingByAscending(config['KeyWhenSortingByAscending'])
        pstep.setMainClassName(config['MainClassName'])
        pstep.setUniqueColumnTransformName(config['UniqueColumnTransformName'])
        pstep.setOperatesOnColumns(config['OperatesOnColumns'])
        pstep.setColumnTransformFilePath(config['ColumnTransformFilePath'])
        for (parname, parvalue) in config['NamedParameterListToInit'].iteritems():
            pstep.setNamedParameter(parname, parvalue)
        return pstep

    def __init__(self, name):
        super(PipelineStep, self).__init__('pipelines/')
        self._script = ''
        self._scriptpath = '.'
        self._hdfspath = ''
        self._config['Name'] = name
        self._config['KeyWhenSortingByAscending'] = 100
        self._config['MainClassName'] = name
        self._config['UniqueColumnTransformName'] = ''
        self._config['OperatesOnColumns'] = []
        self._config['NamedParameterListToInit'] = {}
        self._config['ColumnTransformFilePath'] = name + '.py'

    def setName(self, name):
        i = name.rfind('.py')
        if( i != -1 ):
            name = name[:i]
        self._config['Name'] = name
        self._config['ColumnTransformFilePath'] = name + '.py'

    def getName(self):
        return self._config['Name']

    def setHDFSPath(self, hdfspath):
        self._hdfspath = hdfspath

    def getHDFSPath(self):
        return self._hdfspath

    def setScriptPath(self, script_pathname):
        self._script = script_pathname
        columnTransformFilePath = script_pathname
        i = script_pathname.rfind('/')
        if( i != -1 ):
            self._scriptpath = script_pathname[:i]
            self._script = script_pathname[i+1:]
            columnTransformFilePath = script_pathname[i+1:]
        self.setColumnTransformFilePath(columnTransformFilePath)

    def getScriptPath(self):
        return (self._scriptpath, self._script)

    def setKeyWhenSortingByAscending(self, keyWhenSortingByAscending):
        self._config['KeyWhenSortingByAscending'] = keyWhenSortingByAscending

    def getKeyWhenSortingByAscending(self):
        return self._config['KeyWhenSortingByAscending']

    def setMainClassName(self, mainClassName):
        self._config['MainClassName'] = mainClassName

    def getMainClassName(self):
        return self._config['MainClassName']

    def setUniqueColumnTransformName(self, uniqueColumnTransformName):
        self._config['UniqueColumnTransformName'] = uniqueColumnTransformName

    def getUniqueColumnTransformName(self):
        return self._config['UniqueColumnTransformName']

    def setOperatesOnColumns(self, operatesOnColumns):
        self._config['OperatesOnColumns'] = operatesOnColumns

    def getOperatesOnColumns(self):
        return self._config['OperatesOnColumns']

    def setColumnTransformFilePath(self, columnTransformFilePath):
        self.setName(columnTransformFilePath)

    def getColumnTransformFilePath(self):
        return self._config['ColumnTransformFilePath']

    def setNamedParameter(self, parname, parvalue):
        self._config['NamedParameterListToInit'][parname] = parvalue

    def getNamedParameter(self, parname):
        return (parname, self._config['NamedParameterListToInit'][parname])

    def install(self):
        if self.getScriptPath()[1] != '':
            stepjsonfilename = self.getName() + '.json'
            with open(stepjsonfilename, 'wb') as stepjsonfile:
                stepjsonfile.write(json.dumps(self._config))
            metadataPath = PipelineResource().uploadStepMetadata('.', stepjsonfilename, self.getName())
            (scriptpath, scriptname) = self.getScriptPath()
            pythonPath = PipelineResource().uploadStepImplementation(scriptpath, scriptname, self.getName())
            i = metadataPath.rfind('/')
            if( i != -1 ):
                self._hdfspath = metadataPath[:i]

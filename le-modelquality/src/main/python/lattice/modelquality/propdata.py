
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from .entitybase import EntityBase
from .entityresource import EntityResource

class PropData(EntityBase):

    @classmethod
    def getAll(cls):
        pds = []
        pdconfigs = EntityResource('propdataconfigs/').getAll()
        for pdconfig in pdconfigs:
            pds.append(cls.createFromConfig(pdconfig))
        return pds

    @classmethod
    def getAllNames(cls):
        return EntityResource('propdataconfigs/').getAllNames()

    @classmethod
    def getByName(cls, name):
        pdconfig = EntityResource('propdataconfigs/').getByName(name)
        return cls.createFromConfig(pdconfig)

    @classmethod
    def createFromConfig(cls, config):
        pd = PropData(config['name'])
        pd.setDataCloudVersion(config['data_cloud_version'])
        pd.setPredefinedSelectionName(config['predefined_selection_name'])
        pd.setExcludePropDataColumns(config['exclude_propdata_columns'])
        return pd

    def __init__(self, name):
        super(PropData, self).__init__('propdataconfigs/')
        ## Not all fields are required; these are the ones that are required.
        self._config['name'] = name
        self._config['data_cloud_version'] = 'Unknown data cloud version'
        self._config['predefined_selection_name'] = 'Unknown getPredefinedSelectionName'
        self._config['exclude_propdata_columns'] = False

    def setName(self, name):
        self._config['name'] = name

    def getName(self):
        return self._config['name']

    def setDataCloudVersion(self, data_cloud_version):
        self._config['data_cloud_version'] = data_cloud_version

    def getDataCloudVersion(self):
        return self._config['data_cloud_version']

    def setPredefinedSelectionName(self, predefined_selection_name):
        self._config['predefined_selection_name'] = predefined_selection_name

    def getPredefinedSelectionName(self):
        if self._config['predefined_selection_name']:
            return self._config['predefined_selection_name']
        return None

    def setExcludePropDataColumns(self, exclude_propdata_columns):
        self._config['exclude_propdata_columns'] = exclude_propdata_columns

    def getExcludePropDataColumns(self):
        if self._config['exclude_propdata_columns']:
            return self._config['exclude_propdata_columns']
        return None


#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from .entitybase import EntityBase
from .entityresource import EntityResource

class Dataset(EntityBase):

    @classmethod
    def getAll(cls):
        dss = []
        dsconfigs = EntityResource('datasets/').getAll()
        for dsconfig in dsconfigs:
            dss.append(cls.createFromConfig(dsconfig))
        return dss

    @classmethod
    def getAllNames(cls):
        return EntityResource('datasets/').getAllNames()

    @classmethod
    def getByName(cls, name):
        dsconfig = EntityResource('datasets/').getByName(name)
        return cls.createFromConfig(dsconfig)

    @classmethod
    def createFromConfig(cls, config):
        ds = Dataset(config['name'])
        ds.setCustomerSpace(config['customer_space'])
        ds.setIndustry(config['industry'])
        ds.setDatasetType(config['dataSetType'])
        ds.setSchemaInterpretation(config['schemaInterpretation'])
        for scoring_data_set in config['scoring_data_sets']:
            ds.appendScoringDataset(scoring_data_set)
        ds.setTrainingHDFSPath(config['training_hdfs_path'])
        if 'test_hdfs_path' in config:
            ds.setTestHDFSPath(config['test_hdfs_path'])
        return ds

    def __init__(self, name):
        super(Dataset, self).__init__('datasets/')
        ## Not all fields are required; these are the ones that are required.
        self._config['name'] = name
        self._config['customer_space'] = 'Unknown customer'
        self._config['industry'] = 'Unknown industry'
        self._config['dataSetType'] = 'FILE'
        self._config['schemaInterpretation'] = 'SalesforceLead'
        self._config['scoring_data_sets'] = []
        self._config['training_hdfs_path'] = '/Pods/Default/Services/ModelQuality/Mulesoft_Migration_LP3_ModelingLead_ReducedRows_20160624_155355.csv'

    def setName(self, name):
        self._config['name'] = name

    def getName(self):
        return self._config['name']

    def setCustomerSpace(self, customer_space):
        self._config['customer_space'] = customer_space

    def getCustomerSpace(self):
        return self._config['customer_space']

    def setIndustry(self, industry):
        self._config['industry'] = industry

    def getIndustry(self):
        return self._config['industry']

    def setDatasetType(self, datasettype):
        if datasettype.upper() not in ['FILE', 'EVENTTABLE']:
            raise ValueError('Unknown dataSetType: {}'.format(datasettype))
        self._config['dataSetType'] = datasettype.upper()

    def getDatasetType(self):
        return self._config['dataSetType']

    def setSchemaInterpretation(self, schemaInterpretation):
        if schemaInterpretation not in ['SalesforceAccount', 'SalesforceLead', 'TestingData']:
            raise ValueError('Unknown schemaInterpretation: {}'.format(schemaInterpretation))
        self._config['schemaInterpretation'] = schemaInterpretation

    def getSchemaInterpretation(self):
        return self._config['schemaInterpretation']

    def appendScoringDataset(self, scoring_data_set):
        if 'data_hdfs_path' not in scoring_data_set or 'name' not in scoring_data_set:
            raise ValueError('scoring_data_set needs "data_hdfs_path" and "name"; has: {}'.format(str(scoring_data_set)))
        self._config['scoring_data_sets'].append(scoring_data_set)

    def getScoringDatasets(self):
        return self._config['scoring_data_sets']

    def setTrainingHDFSPath(self, training_hdfs_path):
        self._config['training_hdfs_path'] = training_hdfs_path

    def getTrainingHDFSPath(self):
        if self._config['training_hdfs_path']:
            return self._config['training_hdfs_path']
        return None

    def setTestHDFSPath(self, test_hdfs_path):
        self._config['test_hdfs_path'] = test_hdfs_path

    def getTestHDFSPath(self):
        if self._config['test_hdfs_path']:
            return self._config['test_hdfs_path']
        return None

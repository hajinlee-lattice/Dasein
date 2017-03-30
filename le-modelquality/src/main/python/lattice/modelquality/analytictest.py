# $LastChangedDate$
# $Rev$
#

from .entitybase import EntityBase
from .analytictestresource import AnalyticTestResource

class AnalyticTest(EntityBase):

    @classmethod
    def getAll(cls):
        ats = []
        atconfigs = AnalyticTestResource().getAll()
        for atconfig in atconfigs:
            ats.append(cls.createFromConfig(atconfig))
        return ats

    @classmethod
    def getAllNames(cls):
        return AnalyticTestResource().getAllNames()

    @classmethod
    def getByName(cls, name):
        atconfig = AnalyticTestResource().getByName(name)
        return cls.createFromConfig(atconfig)

    @classmethod
    def createFromConfig(cls, config):
        at = AnalyticTest(config['name'])
        at.setAnalyticPipelineNames(config['analytic_pipeline_names'])
        at.setDatasetNames(config['dataset_names'])
        at.setAnalyticTestTag(config['analytic_test_tag'])
        at.setAnalyticTestType(config['analytic_test_type'])
        return at

    def __init__(self, name):
        super(AnalyticTest, self).__init__('analytictests/')
        self._config['name'] = name
        self._config['analytic_pipeline_names'] = []
        self._config['dataset_names'] = []
        self._config['analytic_test_tag'] = ''
        self._config['analytic_test_type'] = 'SelectedPipelines'

    def setName(self, name):
        self._config['name'] = name

    def getName(self):
        return self._config['name']

    def setAnalyticPipelineNames(self, apnames):
        self._config['analytic_pipeline_names'] = apnames

    def getAnalyticPipelineNames(self):
        return self._config['analytic_pipeline_names']

    def setDatasetNames(self, dsnames):
        self._config['dataset_names'] = dsnames

    def getDatasetNames(self):
        return self._config['dataset_names']

    def setAnalyticTestTag(self, tag):
        self._config['analytic_test_tag'] = tag

    def getAnalyticTestTag(self):
        return self._config['analytic_test_tag']

    def setAnalyticTestType(self, attype):
        if attype not in ['Production', 'SelectedPipelines']:
            raise ValueError('Unknown analytic_test_type: {}'.format(attype))
        self._config['analytic_test_type'] = attype

    def getAnalyticTestType(self):
        return self._config['analytic_test_type']

    def execute(self):
        return AnalyticTestResource().execute(self._config['name'])

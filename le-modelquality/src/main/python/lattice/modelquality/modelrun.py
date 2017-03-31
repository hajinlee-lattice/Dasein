
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#


from .entitybase import EntityBase
from .modelrunresource import ModelRunResource

class ModelRun(EntityBase):

    @classmethod
    def getAll(cls):
        modelruns = []
        modelrundefs = ModelRunResource().getAll()
        for modelrundef in modelrundefs:
            modelruns.append(cls.createFromConfig(modelrundef))
        return modelruns

    @classmethod
    def getAllNames(cls):
        return ModelRunResource().getAllNames()

    @classmethod
    def getByName(cls, name):
        modelrundef = ModelRunResource().getByName(name)
        return cls.createFromConfig(modelrundef)

    @classmethod
    def createFromConfig(cls, config):
        modelrun = ModelRun(config['name'])
        modelrun.setDescription(config['description'])
        modelrun.setAnalyticPipelineName(config['analytic_pipeline_name'])
        modelrun.setDatasetName(config['dataset_name'])
        if 'analytic_test_name' in config:
            modelrun.setAnalyticTestName(config['analytic_test_name'])
        if 'analytic_test_tag' in config:
            modelrun.setAnalyticTestTag(config['analytic_test_tag'])
        return modelrun

    def __init__(self, name):
        super(ModelRun, self).__init__('modelruns/')
        self._config['name'] = name
        self._config['description'] = name
        self._config['analytic_pipeline_name'] = ''
        self._config['dataset_name'] = ''
        self._config['analytic_test_name'] = ''
        self._config['analytic_test_tag'] = ''

    def setName(self, name):
        self._config['name'] = name

    def getName(self):
        return self._config['name']

    def setDescription(self, description):
        self._config['description'] = description

    def getDescription(self):
        return self._config['description']

    def setAnalyticPipelineName(self, analytic_pipeline_name):
        self._config['analytic_pipeline_name'] = analytic_pipeline_name

    def getAnalyticPipelineName(self):
        return self._config['analytic_pipeline_name']

    def setDatasetName(self, name):
        self._config['dataset_name'] = name

    def getDatasetName(self):
        return self._config['dataset_name']

    def setAnalyticTestName(self, name):
        self._config['analytic_test_name'] = name

    def getAnalyticTestName(self):
        return self._config['analytic_test_name']

    def setAnalyticTestTag(self, tag):
        self._config['analytic_test_tag'] = tag

    def getAnalyticTestTag(self):
        return self._config['analytic_test_tag']

    def getStatus(self):
        return ModelRunResource().getStatus(self.getName())

    def getHDFSDir(self):
        return ModelRunResource().getHDFSDir(self.getName())

    def install(self):
        raise RuntimeError('Not implemented; run \"execute\" method with authentication arguments')

    def execute(self, tenant, username, password):
        return ModelRunResource().create(self._config, tenant, username, password)

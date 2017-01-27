
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from .entitybase import EntityBase
from .entityresource import EntityResource

class Algorithm(EntityBase):

    @classmethod
    def getAll(cls):
        algs = []
        algconfigs = EntityResource('algorithms/').getAll()
        for algconfig in algconfigs:
            algs.append(cls.createFromConfig(algconfig))
        return algs

    @classmethod
    def getAllNames(cls):
        return EntityResource('algorithms/').getAllNames()

    @classmethod
    def getByName(cls, name):
        algconfig = EntityResource('algorithms/').getByName(name)
        return cls.createFromConfig(algconfig)

    @classmethod
    def createFromConfig(cls, config):
        alg = Algorithm(config['name'])
        alg.setScript(config['script'])
        alg.setType(config['type'])
        for m in config['algorithm_property_defs']:
            alg.setProperty(m['name'], [v['value'] for v in m['algorithm_property_values']])
        return alg

    def __init__(self, name):
        super(Algorithm, self).__init__('algorithms/')
        self._config['name'] = name
        self._config['type'] = 'RANDOMFOREST'
        self._config['script'] = '/app//dataplatform/scripts/algorithm/rf_train.py'
        self._config['algorithm_property_defs'] = []
        self.setProperty('n_estimators'     , ['100'])
        self.setProperty('criterion'        , ['gini'])
        self.setProperty('n_jobs'           , ['5'])
        self.setProperty('min_samples_split', ['25'])
        self.setProperty('min_samples_leaf' , ['10'])
        self.setProperty('max_depth'        , ['8'])
        self.setProperty('bootstrap'        , ['True'])
        self.setProperty('calibration_width', ['4'])
        self.setProperty('cross_validation' , ['5'])

    def setName(self, name):
        self._config['name'] = name

    def getName(self):
        return self._config['name']

    def setType(self, type):
        self._config['type'] = type

    def getType(self):
        return self._config['type']

    def setScript(self, script):
        self._config['script'] = script

    def getScript(self):
        return self._config['script']

    def setProperty(self, property_name, property_values):
        self._config['algorithm_property_defs'][:] = \
                [m for m in self._config['algorithm_property_defs'] if m['name'] != property_name]
        algorithm_property_values = [{'value':v} for v in property_values]
        algorithm_property_def = {'name':property_name, 'algorithm_property_values':algorithm_property_values}
        self._config['algorithm_property_defs'].append(algorithm_property_def)

    def getProperties(self):
        properties = {}
        for m in self._config['algorithm_property_defs']:
            properties[m['name']] = [v['value'] for v in m['algorithm_property_values']]
        return properties

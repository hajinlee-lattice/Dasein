
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from .entitybase import EntityBase
from .entityresource import EntityResource

class Sampling(EntityBase):

    @classmethod
    def getAll(cls):
        samplings = []
        sconfigs = EntityResource('samplingconfigs/').getAll()
        for sconfig in sconfigs:
            samplings.append(cls.createFromConfig(sconfig))
        return samplings

    @classmethod
    def getAllNames(cls):
        return EntityResource('samplingconfigs/').getAllNames()

    @classmethod
    def getByName(cls, name):
        sconfig = EntityResource('samplingconfigs/').getByName(name)
        return cls.createFromConfig(sconfig)

    @classmethod
    def createFromConfig(cls, config):
        sampling = Sampling(config['name'])
        for m in config['sampling_property_defs']:
            sampling.setProperty(m['name'], [v['value'] for v in m['sampling_property_values']])
        return sampling

    def __init__(self, name):
        super(Sampling, self).__init__('samplingconfigs/')
        self._config['name'] = name
        self._config['sampling_property_defs'] = []

    def setName(self, name):
        self._config['name'] = name

    def getName(self):
        return self._config['name']

    def setProperty(self, property_name, property_values):
        self._config['sampling_property_defs'][:] = \
                [m for m in self._config['sampling_property_defs'] if m['name'] != property_name]
        sampling_property_values = [{'value':v} for v in property_values]
        sampling_property_def = {'name':property_name, 'sampling_property_values':sampling_property_values}
        self._config['sampling_property_defs'].append(sampling_property_def)

    def getProperties(self):
        properties = {}
        for m in self._config['sampling_property_defs']:
            properties[m['name']] = [v['value'] for v in m['sampling_property_values']]
        return properties

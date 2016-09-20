
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from .entitybase import EntityBase
from .entityresource import EntityResource

class Dataflow(EntityBase):

    @classmethod
    def getAll(cls):
        dfs = []
        dfconfigs = EntityResource('dataflows/').getAll()
        for dfconfig in dfconfigs:
            dfs.append(cls.createFromConfig(dfconfig))
        return dfs

    @classmethod
    def getAllNames(cls):
        return EntityResource('dataflows/').getAllNames()

    @classmethod
    def getByName(cls, name):
        dfconfig = EntityResource('dataflows/').getByName(name)
        return cls.createFromConfig(dfconfig)

    @classmethod
    def createFromConfig(cls, config):
        df = Dataflow(config['name'])
        df.setMatch(config['match'])
        df.setTransformDedupType(config['transform_dedup_type'])
        df.setTransformGroup(config['transform_group'])
        return df

    def __init__(self, name):
        super(Dataflow, self).__init__('dataflows/')
        self._config['name'] = name
        self._config['match'] = True
        self._config['transform_dedup_type'] = 'ONELEADPERDOMAIN'
        self._config['transform_group'] = 'STANDARD'

    def setName(self, name):
        self._config['name'] = name

    def getName(self):
        return self._config['name']

    def setMatch(self, match):
        self._config['match'] = match

    def getMatch(self):
        return self._config['match']

    def setTransformDedupType(self, transform_dedup_type):
        if transform_dedup_type.upper() not in ['ONELEADPERDOMAIN', 'MULTIPLELEADSPERDOMAIN']:
            raise ValueError('Unknown transform_dedup_type: {}'.format(transform_dedup_type))
        self._config['transform_dedup_type'] = transform_dedup_type.upper()

    def getTransformDedupType(self):
        return self._config['transform_dedup_type']

    def setTransformGroup(self, transform_group):
        if transform_group.upper() not in ['STANDARD', 'NONE', 'POC', 'ALL']:
            raise ValueError('Unknown transform_group: {}'.format(transform_group))
        self._config['transform_group'] = transform_group.upper()

    def getTransformGroup(self):
        return self._config['transform_group']

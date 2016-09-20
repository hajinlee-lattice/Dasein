
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from abc import ABCMeta, abstractmethod
import json
from .entityresource import EntityResource

class EntityBase(object):
    __metaclass__ = ABCMeta

    def __init__(self, resource):
        self._entityresource = EntityResource(resource)
        self._config = {}

    def getEntityResource(self):
        return self._entityresource

    def printConfig(self):
        print json.dumps(self._config, indent=4)

    def save(self):
        return self.getEntityResource().create(self._config)

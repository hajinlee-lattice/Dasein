
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from .ConnectionMgrVDBImpl import ConnectionMgrVDBImpl
from .exceptions import UnknownDatabaseType

class ConnectionMgrFactory(object):

    @classmethod
    def Create(cls, type, **kwargs):
        if type == 'visiDB':
            return ConnectionMgrVDBImpl(type, **kwargs)
        else:
            raise UnknownDatabaseType(type)
        return None

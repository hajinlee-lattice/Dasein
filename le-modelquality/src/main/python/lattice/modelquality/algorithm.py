
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from .entitybase import EntityBase
from .envconfig import EnvConfig

class Algorithm(EntityBase):

    def __init__(self, envconfig):
        super(Algorithm, self).__init__(envconfig, 'algorithms/')

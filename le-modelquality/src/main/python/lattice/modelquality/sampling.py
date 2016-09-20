
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from .entitybase import EntityBase
from .envconfig import EnvConfig

class Sampling(EntityBase):

    def __init__(self, envconfig):
        super(Sampling, self).__init__(envconfig, 'samplingconfigs/')

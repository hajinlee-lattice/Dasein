
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import datetime
from unittest import TestCase

from modelquality import *

class EntityResourceTest(TestCase):

    def testDataflowResource(self):

        envcfg = EnvConfig('devel', verbose=True)
        dfresource = EntityResource(envcfg, 'dataflows/')

        dataflowname = 'testDataflowResource_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

        print ''
        print 'Creating new Dataflow \"{}\"'.format(dataflowname)
        dataflow = {}
        dataflow['match'] = True
        dataflow['name'] = dataflowname
        dataflow['transform_dedup_type'] = 'ONELEADPERDOMAIN'
        dataflow['transform_group'] = 'STANDARD'
        createdname = dfresource.create(dataflow)
        assert(createdname==dataflowname)

        print ''
        print 'Listing all Dataflows:'
        for dfname in dfresource.getAllNames():
            print ' * {}'.format(dfname)

        print ''
        print 'Getting Dataflow \"{}\"'.format(dataflowname)
        dataflow_returned = dfresource.getByName(dataflowname)
        for par in dataflow_returned:
            assert(dataflow_returned[par]==dataflow[par])
            print '{0} = {1}'.format(par, dataflow_returned[par])

#!/usr/bin/env python

def main():
    import datetime, sys, os
    sys.path.append(os.path.join(os.path.dirname(__file__),'..'))

    from lattice.modelquality import *

    EnvConfig('devel', verbose=True)

    dataflowname = 'testDataflowResource_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    print ''
    print 'Creating new Dataflow \"{}\"'.format(dataflowname)
    dataflow = Dataflow(dataflowname)
    dataflow.save()

    datasetname = 'testDatasetResource_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    print ''
    print 'Creating new Dataset \"{}\"'.format(datasetname)
    dataset = Dataset(datasetname)
    dataset.save()

    propdataname = 'testPropDataResource_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    print ''
    print 'Creating new PropData \"{}\"'.format(propdataname)
    propdata = PropData(propdataname)
    propdata.save()


    print ''
    print 'Listing all Dataflows:'
    for dfname in Dataflow.getAllNames():
        print ' * {}'.format(dfname)

    print ''
    print 'Listing all Datasets:'
    for name in Dataset.getAllNames():
        print ' * {}'.format(name)

    print ''
    print 'Listing all PropDataConfigs:'
    for name in PropData.getAllNames():
        print ' * {}'.format(name)


    print ''
    print 'Printing configurations for all Dataflows:'
    for df in Dataflow.getAll():
        df.printConfig()

    print ''
    print 'Printing configurations for all Datasets:'
    for d in Dataset.getAll():
        d.printConfig()

    print ''
    print 'Printing configurations for all PropData:'
    for d in PropData.getAll():
        d.printConfig()

    print ''
    print 'Getting Dataflow \"{}\"'.format(dataflowname)
    dataflow_returned = Dataflow.getByName(dataflowname)
    dataflow_returned.printConfig()

    print ''
    print 'Getting Dataset \"{}\"'.format(datasetname)
    dataset_returned = Dataset.getByName(datasetname)
    dataset_returned.printConfig()

    print ''
    print 'Getting PropData \"{}\"'.format(propdataname)
    propdata_returned = PropData.getByName(propdataname)
    propdata_returned.printConfig()


if __name__ == "__main__":
    main()


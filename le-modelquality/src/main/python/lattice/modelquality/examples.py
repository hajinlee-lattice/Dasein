#!/usr/local/bin/python

from lattice.modelquality import *

def example_create_dataset():

    dataset = Dataset('hostingcom')
    dataset.setLocalModelingFileName('/Users/michaelwilson/Downloads/hostingcom_rowsremoved.csv')
    ### Other options to set -- not necessary
    #dataset.setCustomerSpace('CustomerName')
    #dataset.setIndustry('Customerindustry')
    ### Can be FILE or EVENTTABLE
    #dataset.setDatasetType('FILE')
    ### Can be 'SalesforceAccount' or 'SalesforceLead'
    #dataset.setSchemaInterpretation('SalesforceAccount')
    #scoring_data_set = {'name':'somename', data_hdfs_path':'/some/path/to/scoring/file'}
    #dataset.append(scoring_data_set)
    dataset.install()

def example_create_pipeline():

    pipeline = Pipeline.getByName('PRODUCTION-')
    pipeline.setName('MGWCustom')

    newstep = PipelineStep('remediatedatarulesstep_custom')
    newstep.setScriptPath('remediatedatarulesstep_custom.py')
    newstep.setMainClassName('RemediateDataRulesStepCustom')
    newstep.setNamedParameter('params', 'params')
    newstep.setNamedParameter('enabledRules', {'PopulatedRowCountDS': []})

    steps = []
    for step in pipeline.getPipelineSteps():
        if step.getName() == 'remediatedatarulesstep':
            steps.append(newstep)
        else:
            steps.append(step)

    pipeline.setPipelineSteps(steps)
    pipeline.install()

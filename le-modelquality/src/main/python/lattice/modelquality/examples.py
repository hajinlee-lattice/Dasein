
from lattice.modelquality import *

EnvConfig('qa')

def create_dataset():

    existingDatasetName = 'PolyCom_domain_AccountMaster'
    newDatasetName = 'newDatasetName'

    dataset = Dataset.getByName(existingDatasetName)
    dataset.printConfig()

    # dataset.setName(newDatasetName)

    ## To upload a local file to HDFS, use setLocalModelingFileName(...).  The file will be uploaded
    ## when you call install().  A file must be uploaded before it can be used by the framework.
    # dataset.setLocalModelingFileName('/Users/michaelwilson/Downloads/hostingcom_rowsremoved.csv')

    ### Other options to set -- not necessary

    # dataset.setCustomerSpace('CustomerName')
    # dataset.setIndustry('Customerindustry')

    ## Can be FILE or EVENTTABLE
    # dataset.setDatasetType('FILE')

    ## Can be 'SalesforceAccount' or 'SalesforceLead'
    # dataset.setSchemaInterpretation('SalesforceAccount')

    # scoring_data_set = {'name':'somename', data_hdfs_path':'/some/path/to/scoring/file'}
    # dataset.append(scoring_data_set)

    ## This step installs a new dataset entity into the framework.  After this step, it can be used.
    # dataset.install()

def create_pipeline():

    existingPipelineName = 'PRODUCTION-b_2.0.43-SNAPSHOT'
    newPipelineName = 'newPipelineName'

    pipeline = Pipeline.getByName(existingPipelineName)
    pipeline.printConfig()

    # pipeline.setName(newPipelineName)

    # newstep = PipelineStep('remediatedatarulesstep_custom')
    # newstep.setScriptPath('remediatedatarulesstep_custom.py')
    # newstep.setMainClassName('RemediateDataRulesStepCustom')
    # newstep.setUniqueColumnTransformName('')
    # newstep.setOperatesOnColumns([])
    # newstep.setNamedParameter('params', 'params')
    # newstep.setNamedParameter('enabledRules', {'PopulatedRowCountDS': []})

    ## This is a list of the existing steps; you can manipulated them as you like
    # steps = pipeline.getPipelineSteps():

    # pipeline.setPipelineSteps(steps)
    # pipeline.install()

def create_analyticpipeline():

    existingAnalyticPipelineName = 'PRODUCTION-2.0.44-SNAPSHOT'
    newAnalyticPipelineName = 'newAnalyticPipelineName'

    analyticPipeline = AnalyticPipeline.getByName(existingAnalyticPipelineName)
    analyticPipeline.printConfig()

    # analyticPipeline.setName(newAnalyticPipelineName)

    # analyticPipeline.setAlgorithmName('')
    # analyticPipeline.setDataflowName('')
    # analyticPipeline.setPipelineName('')
    # analyticPipeline.setPropDataName('')
    # analyticPipeline.setSamplingName('')

    # analyticPipeline.install()

def create_modelrun():

    existingModelRunName = 'modelRun1'
    newModelRunName = 'newModelRunName'
    description = 'A description of my run'

    tenant = 'ModelQualityExperiments.ModelQualityExperiments.Production'
    username = 'bnguyen@lattice-engines.com'
    password = 'tahoe'

    modelRun = ModelRun.getByName(existingModelRunName)
    modelRun.printConfig()

    # modelRun.setName(newModelRunName)
    # modelRun.setDescription(description)

    # modelRun.setAnalyticPipelineName('')
    # modelRun.setDatasetName('')
    # modelRun.execute(tenant, username, password)

def create_dataflow():

    existingDataflowName = 'PRODUCTION-a_2.0.44-SNAPSHOT'
    newDataflowName = 'newDataflowName'

    dataflow = Dataflow.getByName(existingDataflowName)
    dataflow.printConfig()

    # dataflow.setName(newDataflowName)

    ## Match can be either True or False
    # dataflow.setMatch(False)

    ## TransformDedupType can be either 'ONELEADPERDOMAIN' or 'MULTIPLELEADSPERDOMAIN'
    # dataflow.setTransformDedupType('ONELEADPERDOMAIN')

    ## TransformGroup can be 'STANDARD', 'NONE', 'POC', or 'ALL'
    # dataflow.setTransformGroup('POC')

    # dataflow.install()

def create_propdata():

    existingPropDataName = 'PRODUCTION-a_2.0.44-SNAPSHOT'
    newPropDataName = 'newPropDataName'

    propdata = PropData.getByName(existingPropDataName)
    propdata.printConfig()

    propdata.setName(newPropDataName)

    ## The legacy version is 1.0.0; the new matching algorithm is 2.0.0
    # propdata.setDataCloudVersion('2.0.0')

    ## A boolean indicating whether PropData should be included or not
    # propdata.setExcludePropDataColumns(True)

    # propdata.setPredefinedSelectionName('')

    # propdata.install()

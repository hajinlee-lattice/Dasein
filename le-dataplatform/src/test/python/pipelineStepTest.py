import sys, json

from trainingtestbase import TrainingTestBase

class PipelineStepTest(TrainingTestBase):

    def testPipelineStep(self):

        jsonFile = './imputationstep_alt.json'
        config = {}
        with open(jsonFile) as configfile:
            config = json.loads(configfile.read())
        mainClassName = config['MainClassName']
        pipelineConfig = {'columnTransformFiles': {mainClassName:config}}
        with open('temppipeline.json', mode='wb') as pipelineConfigFile:
            pipelineConfigFile.write(json.dumps(pipelineConfig))

        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        pipelinelauncher = Launcher("pipelineStepTest_metadata.json")
        parser = pipelinelauncher.getParser()
        schema = parser.getSchema()
        schema['pipeline_driver'] = 'temppipeline.json'
        pipelinelauncher.execute(writeToHdfs=False, validateEnv=False, postProcessClf=False)

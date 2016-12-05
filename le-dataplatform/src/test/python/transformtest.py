
import os, sys, shutil
from trainingtestbase import TrainingTestBase
import logging
from unittest import TestCase

logging.basicConfig(level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(name='transformtest')

class TransformTest(TestCase):

    @classmethod
    def setUpClass(cls):
        logger.info("=========Current test: " + str(cls) + " ===========")
        cls.basePath = os.path.join('..','..') if os.path.exists(os.path.join('..','..','main','python','rulefwk.py')) else os.path.join('..','..','..')

    @classmethod
    def tearDownClass(cls):
        logger.info("=========Tear down test: " + str(cls) + " ===========")

    def testAddTitleAttributesTrf(self):
        trffile = 'addtitleattributestrf.py'
        artifacts = 'dstitleimputations.json'
        shutil.copy(os.path.join(self.basePath, 'main', 'python', 'configurablepipelinetransformsfromfile', trffile), '.')
        shutil.copy(os.path.join('data',artifacts), '.')
        import addtitleattributestrf as trf

        valuesToCheck = {'CEO': 3.0, '': 15.06, None: 15.06, 'AStringThatIsLongerThanThirtyCharacters': 30.0}
        args = {'column1': 'Title', 'column2': 'DS_TitleLength'}

        for k, v in valuesToCheck.iteritems():
            record = {'Title': k}
            result = trf.transform(args, record)
            self.assertEquals(result, v)

        os.remove(trffile)
        os.remove(artifacts)

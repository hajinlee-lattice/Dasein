from unittest import TestCase

from extractor.danteextractor import DanteExtractor

class DanteExtractorTest(TestCase):

    def testExtractData(self):
        options = {'modelname': 'Saba', 'passwd': 'welcome', 'db': 'DanteDB', 'dsn': 'tynamods', \
                   'outfile': 'outfile.txt', 'user': 'root', 'tenant': 'WTDante'}
        de = DanteExtractor()
        de.extractData(options)

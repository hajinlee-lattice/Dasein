
# $LastChangedBy$
# $LastChangedDate$
# $Rev$

import os
import logging
import fastavro as avro
import pandas as pd


class DataRuleEventTable(object):

    datapath_default = os.path.join(\
            os.path.dirname(__file__), '..', '..', '..', '..', '..',\
            'le-testframework', 'src', 'test', 'data', 'LP3EventTables')


    def __init__(self, name, filename, datapath=datapath_default, loggername='AllDataRuleTests'):

        self.filetype = 'unknown'
        filetype_idx = filename.rfind('.')
        if filetype_idx != -1:
            self.filetype = filename[filetype_idx+1:]

        self.datapath = datapath
        self.filename = filename

        self.name = name
        self.df = None
        self.n_dfrows = -1
        self.includedCols = []
        self.allColsDict = {}
        self.categoricalCols = {}
        self.numericalCols = {}
        self.eventCol = None
        self.logger = logging.getLogger(name=loggername)

        with open(os.path.join(self.datapath, self.filename), mode='rb') as file:
            if self.filetype.lower() == 'avro':
                reader = avro.reader(file)
                schema = reader.schema
                fields = schema['fields']
                for col in fields:
                    if 'InterfaceName' in col.keys() and col['InterfaceName'] == 'Event':
                        self.includedCols.append(col['name'])
                        self.eventCol = col['name']
                    if col['ApprovedUsage'] == '[None]':
                        continue
                    self.allColsDict[col['name']] = None
                    self.includedCols.append(col['name'])
                    datatype = col['type'][0]
                    if datatype in ['int','long','double','boolean']:
                        self.numericalCols[col['name']] = None
                    elif datatype in ['string']:
                        self.categoricalCols[col['name']] = None
            else:
                raise ValueError('Filetype {} not supported'.format(filetype))

    def getName(self):
        return self.name

    def getAllColsAsDict(self):
        return self.allColsDict

    def getCategoricalCols(self):
        return self.categoricalCols

    def getNumericalCols(self):
        return self.numericalCols

    def getEventCol(self):
        return self.eventCol

    def getDataFrame(self, nrows=-1):
        if self.df is None or nrows != self.n_dfrows:
            self._readData(nrows)
            self.n_dfrows = nrows
        return self.df

    def _readData(self, nrows):
        with open(os.path.join(self.datapath, self.filename), mode='rb') as file:
            if self.filetype.lower() == 'avro':
                reader = avro.reader(file)
                rows = []
                i = 0
                self.logger.info('Reading file {}...'.format(self.filename))
                self.df = pd.DataFrame()
                for record in reader:
                    i+=1
                    if nrows > -1 and i > nrows:
                        i-=1
                        break
                    reduced = {cname:record[cname] for cname in self.includedCols}
                    rows.append(reduced)
                    if (i % 5000 == 0):
                        self.logger.info('* Read {} records'.format(i))
                        self.df = self.df.append(rows, ignore_index=True)
                        rows = []
                self.logger.info('Finished reading file; {} records read in total'.format(i))
                self.df = self.df.append(rows, ignore_index=True)

            else:
                raise ValueError('Filetype {} not supported'.format(filetype))

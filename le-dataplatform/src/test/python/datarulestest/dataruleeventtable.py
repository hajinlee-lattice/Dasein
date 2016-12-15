
# $LastChangedBy$
# $LastChangedDate$
# $Rev$

import os
import logging
import fastavro as avro
import pandas as pd
import json


class DataRuleEventTable(object):

    datapath_default = os.path.join(os.path.dirname(__file__), '..', 'data', 'datarules')


    def __init__(self, name, filename, keys, columnMetadataFilename, profileFilename, datapath=datapath_default, loggername='AllDataRuleTests'):

        self.filetype = 'unknown'
        filetype_idx = filename.rfind('.')
        if filetype_idx != -1:
            self.filetype = filename[filetype_idx+1:]

        self.datapath = datapath
        self.filename = filename
        self.keys = keys
        self.columnMetadataFilename = columnMetadataFilename
        self.profileFilename = profileFilename

        self.name = name
        self.df = None
        self.n_dfrows = -1
        self.includedCols = []
        self.allColsDict = {}
        self.categoricalCols = {}
        self.numericalCols = {}
        self.eventCol = None
        self.customerCols = set()
        self.logger = logging.getLogger(name=loggername)
        self.columnMetadata = []
        self.profile = dict()

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

        with open(os.path.join(self.datapath, self.columnMetadataFilename), mode='rb') as self.columnMetadataFile:
            self.columnMetadata = json.load(self.columnMetadataFile)['Metadata']

        with open(os.path.join(self.datapath, self.profileFilename), mode='rb') as profileFile:
            reader = avro.reader(profileFile)
            depivoted = False
            for record in reader:
                colname = record["barecolumnname"]
                sqlcolname = ""
                record["hashValue"] = None
                if record["Dtype"] == "BND":
                    sqlcolname = colname + "_Continuous"  if depivoted else colname
                elif depivoted:
                    sqlcolname = colname + "_" + record["columnvalue"]
                else:
                    sqlcolname = colname
                    record["hashValue"] = record["columnvalue"]

                if colname in self.profile:
                    self.profile[colname].append(record)
                else:
                    self.profile[colname] = [record]

        for column in self.columnMetadata:
            if column['Tags'] is None or column['ColumnName'] is None:
                continue
            if column['ColumnName'] in self.keys or column['ColumnName'] == self.eventCol:
                continue
            if column['Tags'][-1] in ['Internal']:
                self.customerCols.add(column['ColumnName'])

    def getName(self):
        return self.name

    def getAllColsAsDict(self):
        return self.allColsDict

    def getCustomerCols(self):
        return self.customerCols

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

    def getColumnMetadata(self):
        return self.columnMetadata

    def getProfile(self):
        return self.profile

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

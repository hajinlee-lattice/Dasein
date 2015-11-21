#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#
import string

import sys, re
from lxml import etree
from liaison import *


def FormatSpecs(fh_raw):
  line_nladded = ''
  lines_sorted = []
  sortedLinesDict = {}
  fh_raw = string.split(fh_raw,'\n')
  line_nladded = ''
  lines_sorted = []
  sortedLinesDict = {}
  for line_raw in fh_raw:
    line_raw = re.sub('<specs>','<specs>\n',line_raw)
    line_raw = re.sub('SpecLatticeNamedElements\(\(','SpecLatticeNamedElements(( ',line_raw)
    line_raw = re.sub(' SpecLatticeNamedElement','\n    SpecLatticeNamedElement',line_raw)
    #A001 Sort the specs
    if re.search("<specs>",line_raw):
      lines = string.split(line_raw, '\n')
      #Remove ))</specs> at the last element for sorting, add it back after sort
      lines[len(lines)-1] = re.sub('\)\)</specs>',',',lines[len(lines)-1])

      #Begin to create the dict for the sorting
      dict_lines = {}
      #A0002 sort by the ContainerElement
      for line in lines[1:len(lines)]:
        key = re.search('.*\, ContainerElementName\(\"(.*?)\"\).*', line)
        if not key :
          print "Parsing \""+line+"\" failed!"
          continue
        dict_lines[key.group(1)] = line

      Ks = list(dict_lines.keys())
      Ks.sort()
      i = 2
      for key in Ks:
        print key
        lines[i] = dict_lines.get(key)
        i+=1

      #Add ))</specs> back at the last element
      lines[len(lines)-1] = re.sub(',$','))</specs>',lines[len(lines)-1])
      line_raw = '\n'.join(lines)
      # Keep the 1st SpecLatticeNamedElement is behind SpecLatticeNamedElements((
      line_raw = re.sub('SpecLatticeNamedElements\(\(\n    ','SpecLatticeNamedElements((',line_raw)
    line_raw += '\n'
    line_nladded += line_raw
  lines = string.split(line_nladded,'\n')
  n_lines = 0
  indent = 0
  alltext_format = ''
  for line in lines:
    n_lines += 1
    i = 0
    line_fmt = ''
    in_quotes = False
    last_c_was_end_paren = False
    c = ''
    while i < len(line):
      b = c
      c, i = __get_next_char( line, i )
      if c == u'"' and b != '\\':
        in_quotes = not in_quotes
      if not in_quotes:
        if c == u'(':
          last_c_was_end_paren = False
          line_fmt += c
          indent += 2
          line_fmt += '\n'
          line_fmt += ' '*indent
        elif c == u')':
          indent -= 2
          if not last_c_was_end_paren:
            line_fmt += '\n'
            line_fmt += ' '*indent
          line_fmt += c
          line_fmt += '\n'
          d, j = __get_next_char( line, i )
          if d == u',':
            line_fmt += ' '*(indent-2)
          elif d == u' ':
            line_fmt += ' '*(indent-1)
            #line_fmt += 'HERE'
          elif d == u')':
            line_fmt += ' '*(indent-2)
          else:
            line_fmt += ' '*indent
          last_c_was_end_paren = True
        else:
          last_c_was_end_paren = False
          line_fmt += c
      else:
        line_fmt += c
    line_fmt += '\n'
    alltext_format += line_fmt
  return alltext_format


def __get_next_char(s, i):
  return s[i], i + 1


def downloadSpecsAndConfig(tenantName, fileNameBase):
  specFileName = fileNameBase + '.specs'
  configFileName = fileNameBase + '.config'

  conn_mgr = ConnectionMgrFactory.Create('visiDB', tenant_name=tenantName)
  lg_mgr = conn_mgr.getLoadGroupMgr()

  specs = conn_mgr.getAllSpecs()
  hearder = '<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<VisiDBStructures appName=\"\">\n  <workspaces>\n    <workspace '\
             'name=\"Workspace\">\n      <specs>'
  tailer = '</specs>\n    </workspace>\n  </workspaces>\n</VisiDBStructures>'
  specs = hearder + specs + tailer

  formatedSpecs = FormatSpecs(specs)

  with open(specFileName, mode='w') as specFile:
    specFile.write(formatedSpecs)

  if type == 'ELQ':
    lg_mgr.createDataProvider(
      '<dataProvider name="Eloqua_Bulk_DataProvider" autoMatch="False" '
      'connectionString="URL=https://login.eloqua.com/id;EntityType=Base;Timeout=100;RetryTimesForTimeout=3;BatchSize'
      '=10000" dbType="1008" usedFor="1" e="False" />')
    lg_mgr.createDataProvider(
      '<dataProvider name="Eloqua_DataProvider" autoMatch="False" '
      'connectionString="URL=https://login.eloqua.com/id;EntityType=Base;Timeout=100;RetryTimesForTimeout=3;BatchSize'
      '=200" dbType="1006" usedFor="31" e="False" />')
  elif type == 'MKTO':
    lg_mgr.createDataProvider(
      '<dataProvider name="Marketo_DataProvider" autoMatch="False" '
      'connectionString="URL=;UserID=;EncryptionKey=;Timeout=10000;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60'
      ';BatchSize=500;MaxSizeOfErrorBatch=25;" dbType="1004" usedFor="31" e="False" />')

  if type in ['ELQ', 'MKTO', 'SFDC']:
    lg_mgr.createDataProvider(
      '<dataProvider name="SFDC_DataProvider" autoMatch="False" '
      'connectionString="URL=https://login.salesforce.com/services/Soap/u/27.0;User=;Password=;SecurityToken=;Version'
      '=27.0;Timeout=100;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=2000;" dbType="1002" usedFor="31" '
      'e="False" />')
    lg_mgr.createDataProvider(
      '<dataProvider name="SQL_DanteDB_DataProvider" autoMatch="False" '
      'connectionString="ServerName=;Database=;User=;Password=;Authentication=SQL Server '
      'Authentication;Schema=dbo;BatchSize=2000" dbType="2" usedFor="1" e="False" />')
    lg_mgr.createDataProvider(
      '<dataProvider name="SQL_LeadScoring" autoMatch="False" '
      'connectionString="ServerName=BODCPRODVSQL229;Authentication=SQL Server '
      'Authentication;User=dataloader_prod;Password=L@ttice2;Database=LeadScoringDB;Schema=dbo;DateTimeOffsetOption'
      '=UtcDateTime;Timeout=3600;RetryTimesForTimeout=10;SleepTimeBeforeRetry=60;BatchSize=2000;" dbType="2" '
      'usedFor="10" e="False" />')
    lg_mgr.createDataProvider(
      '<dataProvider name="SQL_LeadValidation_DataProvider" autoMatch="False" '
      'connectionString="ServerName=BODCPRODVSQL200\SQL200;Authentication=SQL Server '
      'Authentication;User=dataloader_prod;Password=L@ttice2;Database=LeadValidationDB;Schema=dbo'
      ';DateTimeOffsetOption=UtcDateTime;Timeout=3600;RetryTimesForTimeout=10;SleepTimeBeforeRetry=60;BatchSize=2000'
      ';" dbType="2" usedFor="31" e="False" et="2" />')
    lg_mgr.createDataProvider(
      '<dataProvider name="SQL_LoadGroupsMeta" autoMatch="False" '
      'connectionString="ServerName=;Database=;User=;Password=;Authentication=SQL Server '
      'Authentication;Schema=dbo;BatchSize=2000" dbType="2" usedFor="10" e="False" />')
    lg_mgr.createDataProvider(
      '<dataProvider name="SQL_LSSBard" autoMatch="False" '
      'connectionString="ServerName=BODCPRODVSQL200.prod.lattice.local\SQL200;Database=Scoring_Daemon_PLS_2;User=s'
      '-scoring;Password=Sk03ing;Authentication=SQL Server '
      'Authentication;Schema=dbo;Timeout=3600;RetryTimesForTimeout=10;BatchSize=2000" dbType="2" usedFor="10" '
      'e="False" />')
    lg_mgr.createDataProvider(
      '<dataProvider name="SQL_Meta" autoMatch="False" '
      'connectionString="ServerName=;Database=;User=;Password=;Authentication=SQL Server '
      'Authentication;Schema=dbo;BatchSize=2000" dbType="2" usedFor="10" e="False" />')
    lg_mgr.createDataProvider(
      '<dataProvider name="SQL_MultiTenant" autoMatch="False" '
      'connectionString="ServerName=BODCPRODVSQL100.prod.lattice.local\SQL100;Database=PLS_MultiTenant;User=s'
      '-multitenant;Password=M51+Eye10ant;Authentication=SQL Server '
      'Authentication;Schema=dbo;Timeout=3600;RetryTimesForTimeout=10;BatchSize=2000" dbType="2" usedFor="10" '
      'e="False" />')
    lg_mgr.createDataProvider(
      '<dataProvider name="SQL_POC_EventTableRepository" autoMatch="False" '
      'connectionString="ServerName=le-spa;Authentication=SQL Server '
      'Authentication;User=dataloader_dep;Password=L@ttice1;Database=POC_EventTableRepository;Schema=dbo'
      ';DateTimeOffsetOption=UtcDateTime;Timeout=100;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=2000;" '
      'dbType="2" usedFor="31" e="False" />')
    lg_mgr.createDataProvider(
      '<dataProvider name="SQL_PropDataForModeling" autoMatch="False" '
      'connectionString="ServerName=BODCPRODVSQL128;Authentication=SQL Server '
      'Authentication;User=dataloader_prod;Password=L@ttice2;Database=PropDataMatchDB;Schema=dbo;DateTimeOffsetOption'
      '=UtcDateTime;Timeout=100;RetryTimesForTimeout=10;SleepTimeBeforeRetry=60;BatchSize=40000;" dbType="2" '
      'usedFor="10" e="False" />')
    lg_mgr.createDataProvider(
      '<dataProvider name="SQL_PropDataForScoring" autoMatch="False" '
      'connectionString="ServerName=BODCPRODVSQL126;Authentication=SQL Server '
      'Authentication;User=dataloader_prod;Password=L@ttice2;Database=PropDataMatchDB;Schema=dbo;DateTimeOffsetOption'
      '=UtcDateTime;Timeout=100;RetryTimesForTimeout=10;SleepTimeBeforeRetry=60;BatchSize=40000;" dbType="2" '
      'usedFor="10" e="False" />')
    lg_mgr.createDataProvider(
      '<dataProvider name="SQL_PropDataMatch" autoMatch="False" '
      'connectionString="ServerName=bodcprodvsql130;Authentication=SQL Server '
      'Authentication;User=dataloader_prod;Password=L@ttice2;Database=PropDataMatchDB;Schema=dbo;DateTimeOffsetOption'
      '=UtcDateTime;Timeout=100;RetryTimesForTimeout=10;SleepTimeBeforeRetry=60;BatchSize=40000;" dbType="2" '
      'usedFor="10" e="False" />')
    lg_mgr.createDataProvider(
      '<dataProvider name="SQL_ProvisioningMeta" autoMatch="False" '
      'connectionString="ServerName=BODCPRODVSQL228;Authentication=SQL Server '
      'Authentication;User=dataloader_prod;Password=L@ttice2;Database=LEDataDB_30;Schema=dbo;DateTimeOffsetOption'
      '=UtcDateTime;Timeout=100;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=2000;" dbType="2" '
      'usedFor="31" e="False" />')
    lg_mgr.createDataProvider(
      '<dataProvider name="SQL_ReportsDB_DataProvider" autoMatch="False" '
      'connectionString="ServerName=;Database=;User=;Password=;Authentication=Windows '
      'Authentication;Schema=dbo;BatchSize=2000" dbType="2" usedFor="1" e="False" />')
    lg_mgr.createDataProvider(
      '<dataProvider name="SQL_SPrism" autoMatch="False" '
      'connectionString="ServerName=;Database=;User=;Password=;Authentication=SQL Server '
      'Authentication;Schema=dbo;BatchSize=2000" dbType="2" usedFor="10" e="False" />')
    lg_mgr.createDataProvider(
      '<dataProvider name="SQL_StagingTableInput" autoMatch="False" '
      'connectionString="ServerName=;Database=;User=;Password=;Authentication=SQL Server '
      'Authentication;Schema=dbo;BatchSize=2000" dbType="2" usedFor="10" e="False" />')

  with open(configFileName, mode='w') as configFile:
    configFile.write(lg_mgr.getConfig(appName=''))


def usage(cmd, exit_code):
  print ''
  print 'Usage: {0} <tenantName> <fileNameBase>'.format(cmd)
  print 'or: '
  print 'Usage: {0} -f <tenant_url.csv>'.format(cmd)
  print ''

  exit(exit_code)


if __name__ == "__main__":

  cmd = ''
  path = ''
  i = sys.argv[0].rfind('\\')
  if (i != -1):
    path = sys.argv[0][:i]
    cmd = sys.argv[0][i + 1:]

  if len(sys.argv) == 1:
    usage(cmd, 0)

  option = sys.argv[1]

  if option not in ['-f']:
    if len(sys.argv) != 3:
      usage(cmd, 1)
    tenantName = sys.argv[1]
    fileNameBase = sys.argv[2]
    downloadSpecsAndConfig(tenantName, fileNameBase)

  else:

    if len(sys.argv) != 3:
      usage(cmd, 1)

    tenantsFile = sys.argv[2]
    with open(tenantsFile) as tenantFile:
      for line in tenantFile:
        cols = line.strip().split(',')
        tenantName = cols[0]
        downloadSpecsAndConfig(tenantName, tenantName)
        print 'Completed tenant: {0}\n'.format(tenantName)

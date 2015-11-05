
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import sys, re
from lxml import etree
from liaison import *

def downloadSpecsAndConfig(tenantName, fileNameBase):
    
    specFileName = fileNameBase+'.specs'
    configFileName = fileNameBase+'.config'

    conn_mgr = ConnectionMgrFactory.Create('visiDB', tenant_name=tenantName)
    lg_mgr = conn_mgr.getLoadGroupMgr()

    specdict = conn_mgr.getSpecDictionary()

    type = 'Unknown'
    if 'Version' in specdict:
        (vdbtype, defn, slne) = specdict['Version']
        c = re.search( 'LatticeFunctionExpressionConstant\(\"PLS (.*?) Template:\".*LatticeFunctionExpressionConstant\(\"(.*?)\"', defn )
        if c:
            type = c.group(1)
        else:
            type = 'Nonstandard type'

    with open( specFileName, mode='w' ) as specFile:

        specFile.write('<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<VisiDBStructures appName=\"\">\n  <workspaces>\n    <workspace name=\"Workspace\">\n      <specs>\n')
        specFile.write('SpecLatticeNamedElements((\n')

        sep = ' '
        for (name,(vdbtype, defn, slne)) in sorted(specdict.iteritems()):
            specFile.write(' {0}SpecLatticeNamedElement(\n'.format(sep))
            specFile.write('    {0}\n'.format(defn))
            specFile.write('  , ContainerElementName(\"{0}\")\n'.format(name))
            specFile.write('  )\n')
            sep = ','

        specFile.write('))\n')
        specFile.write('      </specs>\n    </workspace>\n  </workspaces>\n</VisiDBStructures>')

    if type == 'ELQ':
        lg_mgr.createDataProvider('<dataProvider name="Eloqua_Bulk_DataProvider" autoMatch="False" connectionString="URL=https://login.eloqua.com/id;EntityType=Base;Timeout=100;RetryTimesForTimeout=3;BatchSize=10000" dbType="1008" usedFor="1" e="False" />')
        lg_mgr.createDataProvider('<dataProvider name="Eloqua_DataProvider" autoMatch="False" connectionString="URL=https://login.eloqua.com/id;EntityType=Base;Timeout=100;RetryTimesForTimeout=3;BatchSize=200" dbType="1006" usedFor="31" e="False" />')
    elif type == 'MKTO':
        lg_mgr.createDataProvider('<dataProvider name="Marketo_DataProvider" autoMatch="False" connectionString="URL=;UserID=;EncryptionKey=;Timeout=10000;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=500;MaxSizeOfErrorBatch=25;" dbType="1004" usedFor="31" e="False" />')

    if type in ['ELQ','MKTO','SFDC']:
        lg_mgr.createDataProvider('<dataProvider name="SFDC_DataProvider" autoMatch="False" connectionString="URL=https://login.salesforce.com/services/Soap/u/27.0;User=;Password=;SecurityToken=;Version=27.0;Timeout=100;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=2000;" dbType="1002" usedFor="31" e="False" />')
        lg_mgr.createDataProvider('<dataProvider name="SQL_DanteDB_DataProvider" autoMatch="False" connectionString="ServerName=;Database=;User=;Password=;Authentication=SQL Server Authentication;Schema=dbo;BatchSize=2000" dbType="2" usedFor="1" e="False" />')
        lg_mgr.createDataProvider('<dataProvider name="SQL_LeadScoring" autoMatch="False" connectionString="ServerName=BODCPRODVSQL229;Authentication=SQL Server Authentication;User=dataloader_prod;Password=L@ttice2;Database=LeadScoringDB;Schema=dbo;DateTimeOffsetOption=UtcDateTime;Timeout=3600;RetryTimesForTimeout=10;SleepTimeBeforeRetry=60;BatchSize=2000;" dbType="2" usedFor="10" e="False" />')
        lg_mgr.createDataProvider('<dataProvider name="SQL_LeadValidation_DataProvider" autoMatch="False" connectionString="ServerName=BODCPRODVSQL200\SQL200;Authentication=SQL Server Authentication;User=dataloader_prod;Password=L@ttice2;Database=LeadValidationDB;Schema=dbo;DateTimeOffsetOption=UtcDateTime;Timeout=3600;RetryTimesForTimeout=10;SleepTimeBeforeRetry=60;BatchSize=2000;" dbType="2" usedFor="31" e="False" et="2" />')
        lg_mgr.createDataProvider('<dataProvider name="SQL_LoadGroupsMeta" autoMatch="False" connectionString="ServerName=;Database=;User=;Password=;Authentication=SQL Server Authentication;Schema=dbo;BatchSize=2000" dbType="2" usedFor="10" e="False" />')
        lg_mgr.createDataProvider('<dataProvider name="SQL_LSSBard" autoMatch="False" connectionString="ServerName=BODCPRODVSQL200.prod.lattice.local\SQL200;Database=Scoring_Daemon_PLS_2;User=s-scoring;Password=Sk03ing;Authentication=SQL Server Authentication;Schema=dbo;Timeout=3600;RetryTimesForTimeout=10;BatchSize=2000" dbType="2" usedFor="10" e="False" />')
        lg_mgr.createDataProvider('<dataProvider name="SQL_Meta" autoMatch="False" connectionString="ServerName=;Database=;User=;Password=;Authentication=SQL Server Authentication;Schema=dbo;BatchSize=2000" dbType="2" usedFor="10" e="False" />')
        lg_mgr.createDataProvider('<dataProvider name="SQL_MultiTenant" autoMatch="False" connectionString="ServerName=BODCPRODVSQL100.prod.lattice.local\SQL100;Database=PLS_MultiTenant;User=s-multitenant;Password=M51+Eye10ant;Authentication=SQL Server Authentication;Schema=dbo;Timeout=3600;RetryTimesForTimeout=10;BatchSize=2000" dbType="2" usedFor="10" e="False" />')
        lg_mgr.createDataProvider('<dataProvider name="SQL_POC_EventTableRepository" autoMatch="False" connectionString="ServerName=le-spa;Authentication=SQL Server Authentication;User=dataloader_dep;Password=L@ttice1;Database=POC_EventTableRepository;Schema=dbo;DateTimeOffsetOption=UtcDateTime;Timeout=100;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=2000;" dbType="2" usedFor="31" e="False" />')
        lg_mgr.createDataProvider('<dataProvider name="SQL_PropDataForModeling" autoMatch="False" connectionString="ServerName=BODCPRODVSQL128;Authentication=SQL Server Authentication;User=dataloader_prod;Password=L@ttice2;Database=PropDataMatchDB;Schema=dbo;DateTimeOffsetOption=UtcDateTime;Timeout=100;RetryTimesForTimeout=10;SleepTimeBeforeRetry=60;BatchSize=40000;" dbType="2" usedFor="10" e="False" />')
        lg_mgr.createDataProvider('<dataProvider name="SQL_PropDataForScoring" autoMatch="False" connectionString="ServerName=BODCPRODVSQL126;Authentication=SQL Server Authentication;User=dataloader_prod;Password=L@ttice2;Database=PropDataMatchDB;Schema=dbo;DateTimeOffsetOption=UtcDateTime;Timeout=100;RetryTimesForTimeout=10;SleepTimeBeforeRetry=60;BatchSize=40000;" dbType="2" usedFor="10" e="False" />')
        lg_mgr.createDataProvider('<dataProvider name="SQL_PropDataMatch" autoMatch="False" connectionString="ServerName=bodcprodvsql130;Authentication=SQL Server Authentication;User=dataloader_prod;Password=L@ttice2;Database=PropDataMatchDB;Schema=dbo;DateTimeOffsetOption=UtcDateTime;Timeout=100;RetryTimesForTimeout=10;SleepTimeBeforeRetry=60;BatchSize=40000;" dbType="2" usedFor="10" e="False" />')
        lg_mgr.createDataProvider('<dataProvider name="SQL_ProvisioningMeta" autoMatch="False" connectionString="ServerName=BODCPRODVSQL228;Authentication=SQL Server Authentication;User=dataloader_prod;Password=L@ttice2;Database=LEDataDB_30;Schema=dbo;DateTimeOffsetOption=UtcDateTime;Timeout=100;RetryTimesForTimeout=3;SleepTimeBeforeRetry=60;BatchSize=2000;" dbType="2" usedFor="31" e="False" />')
        lg_mgr.createDataProvider('<dataProvider name="SQL_ReportsDB_DataProvider" autoMatch="False" connectionString="ServerName=;Database=;User=;Password=;Authentication=Windows Authentication;Schema=dbo;BatchSize=2000" dbType="2" usedFor="1" e="False" />')
        lg_mgr.createDataProvider('<dataProvider name="SQL_SPrism" autoMatch="False" connectionString="ServerName=;Database=;User=;Password=;Authentication=SQL Server Authentication;Schema=dbo;BatchSize=2000" dbType="2" usedFor="10" e="False" />')
        lg_mgr.createDataProvider('<dataProvider name="SQL_StagingTableInput" autoMatch="False" connectionString="ServerName=;Database=;User=;Password=;Authentication=SQL Server Authentication;Schema=dbo;BatchSize=2000" dbType="2" usedFor="10" e="False" />')

    with open( configFileName, mode='w' ) as configFile:
        configFile.write(lg_mgr.getConfig(appName=''))


def usage(cmd, exit_code):

    print ''
    print 'Usage: {0} <tenantName> <fileNameBase>'.format(cmd)
    print ''

    exit( exit_code )


if __name__ == "__main__":

    cmd = ''
    path = ''
    i = sys.argv[0].rfind('\\')
    if( i != -1 ):
        path = sys.argv[0][:i]
        cmd = sys.argv[0][i+1:]

    if len(sys.argv) == 1:
        usage( cmd, 0 )

    if len(sys.argv) != 3:
        usage( cmd, 1 )

    tenantName = sys.argv[1]
    fileNameBase = sys.argv[2]

    downloadSpecsAndConfig(tenantName, fileNameBase)


#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

try:
    import requests
    from requests.packages import urllib3
    urllib3.disable_warnings()
except ImportError as ie:
    print ''
    print '!!! Module \'requests\' is necessary for visiDB/DataLoader connectivity in \'liaison\' !!!'
    print ''
    raise ie

try:
    from lxml import etree
except ImportError as ie:
    print ''
    print '!!! Module \'lxml\' is necessary for interpretting visiDB specs and query results in \'liaison\' !!!'
    print ''
    raise ie

import os, datetime, re, htmlentitydefs
from .exceptions import HTTPError, EndpointError, TenantNotMappedToURL, TenantNotFoundAtURL, VisiDBNotFoundOnServer, DataLoaderError
from .exceptions import UnknownVisiDBSpec, UnknownDataLoaderObject, MaudeStringError, XMLStringError
from .ConnectionMgr import ConnectionMgr
from .TableVDBImpl import TableVDBImpl
from .QueryVDBImpl import QueryVDBImpl
from .NamedExpressionVDBImpl import NamedExpressionVDBImpl
from .QueryResultVDBImpl import QueryResultVDBImpl
from .LoadGroupMgrImpl import LoadGroupMgrImpl


class ConnectionMgrVDBImpl(ConnectionMgr):

    def __init__(self, type, tenant_name, dataloader_url = None, verify = True, verbose = False):

        super(ConnectionMgrVDBImpl, self).__init__(type, verbose)

        self._headers = {}
        self._headers['MagicAuthentication'] = 'Security through obscurity!'
        self._headers['Content-Type']        = 'application/json; charset=utf-8'

        if dataloader_url is None:
            dataloader_url = self.__getURLForTenant(tenant_name)

        self._url         = dataloader_url
        self._tenant_name = tenant_name
        self._verify      = verify
        if dataloader_url in ['https://10.41.1.187:8080/','http://10.41.1.207:8081/','https://data-pls2.prod.lattice.local/dataloader/']:
            self._verify = False

        self._lg_mgr = None


    def getTenantName(self):
        return self._tenant_name


    def getTable(self, table_name):

        table_name_import = table_name + '_Import'

        itable_spec = self.getSpec(table_name_import)
        stable_spec = self.getSpec(table_name)

        table = TableVDBImpl.initFromDefn(table_name, stable_spec, itable_spec)

        return table


    def setTable(self, table):

        self.setSpec(table.getName(), table.SpecLatticeNamedElements())


    def getNamedExpression(self, expression_name):

        spec = self.getSpec(expression_name)

        expression = NamedExpressionVDBImpl.InitFromDefn(expression_name, spec)

        return expression


    def setNamedExpression(self, expression):

        self.setSpec(expression.Name(), expression.SpecLatticeNamedElements())


    def getQuery(self, query_name):

        spec = self.getSpec(query_name)

        query = QueryVDBImpl.initFromDefn(query_name, spec)

        return query


    def getMetadata(self, query_name):

        url = self._url + '/DLRestService/GetQueryMetadataColumns'

        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['queryName'] = query_name

        if self.isVerbose():
            print self.getType() +': Getting visiDB Metadata for Query \'{0}\'...'.format( payload['queryName'] ),

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )

        if response.status_code != 200:
            if self.isVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.GetSpec(): POST request returned code {0}'.format(response.status_code) )

        status = None
        errmsg = ''
        try:
            status = response.json()['Status']
            errmsg = response.json()['ErrorMessage']
            mdraw  = response.json()['Metadata']
        except ValueError as e:
            if self.isVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if status != 3:
            if self.isVerbose():
                print 'Failed to get metadata for query \'{0}\''.format( payload['queryName'] )
                print errmsg
            raise TenantNotFoundAtURL( 'ConnectionMgrVDBImpl.GetMetadata(): {0}'.format(payload['tenantName']) )

        if self.isVerbose():
            print 'Success'

        columns = {}

        for col_data in mdraw:

            metadata = {}
            metadata['DisplayName']     = col_data['DisplayName']
            metadata['Description']     = col_data['Description']
            metadata['Tags']            = None
            if col_data['Tags'] is not None and len(col_data['Tags']) > 0:
                metadata['Tags']          = col_data['Tags'][-1]
            metadata['FundamentalType'] = col_data['FundamentalType']
            metadata['DisplayDiscretization'] = col_data['DisplayDiscretizationStrategy']
            metadata['Category']        = None
            if metadata['Tags'] == 'Internal':
                metadata['Category'] = 'Lead Information'
            metadata['DataType']        = None
            if col_data['Extensions'] is not None:
                for ext in col_data['Extensions']:
                    k = ext['Key']
                    v = ext['Value']
                    if k not in ['Category','DataType']:
                        continue
                    metadata[k]             = v
            metadata['DataSource']      = 'Lattice Data Science'
            if col_data['DataSource'] is not None and len(col_data['DataSource']) == 1 and col_data['DataSource'][0] != '':
                metadata['DataSource']     = col_data['DataSource'][0]
            metadata['ApprovedUsage']   = None
            if col_data['ApprovedUsage'] is not None and len(col_data['ApprovedUsage']) > 0:
                metadata['ApprovedUsage'] = col_data['ApprovedUsage'][-1]
            metadata['StatisticalType'] = col_data['StatisticalType']

            columns[ col_data['ColumnName'] ] = metadata

        return columns


    def setQuery(self, query):

        self.setSpec(query.getName(), query.SpecLatticeNamedElements())


    def executeQuery(self, query_name):

        url = self._url + '/DLQueryService/RunQuery'

        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['queryName'] = query_name

        if self.isVerbose():
            print self.getType() +': Executing Query \'{0}\'...'.format( payload['queryName'] ),

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )

        if response.status_code != 200:
            if self.isVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.ExecuteQuery(): POST request returned code {0}'.format(response.status_code) )

        success = False
        errmsg = ''
        try:
            success = response.json()['Success']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.isVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if not success:
            if self.isVerbose():
                print 'Failed to execute query \'{0}\''.format( payload['queryName'] )
                print errmsg
            raise UnknownVisiDBSpec( 'ConnectionMgrVDBImpl.ExecuteQuery(): {0}'.format(payload['queryName']) )

        if self.isVerbose():
            print 'Success'

        return QueryResultVDBImpl(self, response.json()['QueryHandle'])


    def getLoadGroupMgr(self):

        if self._lg_mgr is None:

            url = self._url + '/DLRestService/DownloadConfigFile'

            payload = {}
            payload['tenantName'] = self._tenant_name

            if self.isVerbose():
                print self.getType() +': Getting DataLoader Config File...',

            try:
                response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
            except requests.exceptions.ConnectionError as e:
                raise EndpointError( e )

            if response.status_code != 200:
                if self.isVerbose():
                    print 'HTTP POST request failed'
                raise HTTPError( 'ConnectionMgrVDBImpl.GetLoadGroupMgr(): POST request returned code {0}'.format(response.status_code) )

            status = None
            errmsg = ''
            try:
                status = response.json()['Status']
                errmsg = response.json()['ErrorMessage']
            except ValueError as e:
                if self.isVerbose():
                    print 'HTTP Endpoint did not return the expected response'
                raise EndpointError( e )

            if status != 3:
                if self.isVerbose():
                    print 'Failed to get config file'
                    print errmsg
                raise TenantNotFoundAtURL( 'ConnectionMgrVDBImpl.GetLoadGroupMgr(): {0}'.format(payload['tenantName']) )

            valuedict  = response.json()['Value'][0]
            configfile = valuedict['Value'].encode('ascii', 'xmlcharrefreplace')

            lgm = LoadGroupMgrImpl( self, configfile )

            if self.isVerbose():
                print 'Success'

            self._lg_mgr = lgm

        return self._lg_mgr


    def getSpec(self, spec_name):

        url = self._url + '/DLRestService/GetSpecDetails'

        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['specName'] = spec_name

        if self.isVerbose():
            print self.getType() +': Getting visiDB Spec \'{0}\'...'.format( payload['specName'] ),

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )

        if response.status_code != 200:
            if self.isVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.GetSpec(): POST request returned code {0}'.format(response.status_code) )

        success = False
        errmsg = ''
        try:
            success = response.json()['Success']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.isVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if not success:
            if self.isVerbose():
                print 'Failed to get spec \'{0}\''.format( payload['specName'] )
                print errmsg
            if errmsg == 'Tenant \'{0}\' does not exist.'.format( payload['tenantName'] ):
                raise TenantNotFoundAtURL( 'ConnectionMgrVDBImpl.GetSpec(): {0}'.format(payload['tenantName']) )
            else:
                raise UnknownVisiDBSpec( 'ConnectionMgrVDBImpl.GetSpec(): {0}'.format(payload['specName']) )

        if self.isVerbose():
            print 'Success'

        return response.json()['SpecDetails']


    def getAllSpecs(self):

        url = self._url + '/DLRestService/DownloadVisiDBStructureFile'

        payload = {}
        payload['tenantName'] = self._tenant_name

        if self.isVerbose():
            print self.getType() +': Getting All visiDB Specs...',

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )

        if response.status_code != 200:
            if self.isVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.GetAllSpecs(): POST request returned code {0}'.format(response.status_code) )

        status = None
        errmsg = ''
        try:
            status = response.json()['Status']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.isVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if status != 3:
            if self.isVerbose():
                print 'Failed to get all specs'
                print errmsg
            raise TenantNotFoundAtURL( 'ConnectionMgrVDBImpl.GetAllSpecs(): {0}'.format(payload['tenantName']) )

        valuedict  = response.json()['Value'][0]
        specfile = valuedict['Value'].encode('ascii', 'xmlcharrefreplace')

        specstart_i = specfile.find('<specs>') + 7
        specend_i = specfile.find('</specs>')

        #if specs is None:
        #    raise UnknownVisiDBSpec( 'No specs returned' )

        if self.isVerbose():
            print 'Success'

        return specfile[specstart_i:specend_i]


    def getSpecDictionary(self):

        slne = self.getAllSpecs()

        s1 = re.search( '^SpecLatticeNamedElements\((.*)\)$', slne )
        if not s1:
            if self.isVerbose():
                print 'Tenant \'{0}\' has unrecognizable SpecLatticeNamedElements()'.format( tenant )
            raise UnknownVisiDBSpec( 'ConnectionMgrVDBImpl.getSpecDictionary(): Unrecognizable SpecLatticeNamedElements()' )

        specs_maude = s1.group(1)
        specs = {}

        if specs_maude != 'empty':

            specs_maude = specs_maude[1:-1]

            while True:

                s2 = re.search( '^(SpecLatticeNamedElement.*?)(, SpecLatticeNamedElement.*|$)', specs_maude )
                if not s2:
                    if self.isVerbose():
                        print 'Tenant \'{0}\' has unrecognizable SpecLatticeNamedElement()'.format( tenant )
                    raise UnknownVisiDBSpec( 'ConnectionMgrVDBImpl.getSpecDictionary(): Unrecognizable SpecLatticeNamedElements()' )

                singlespec    = s2.group(1)
                remainingspec = s2.group(2)

                s3 = re.search( 'SpecLatticeNamedElement\(((SpecLattice.*?)\(.*\)), ContainerElementName\(\"(.*?)\"\)\)$', singlespec )
                if not s3:
                    if self.isVerbose():
                        print 'Cannot parse spec for Tenant \'{0}\':\n\n{1}\n'.format( tenant, singlespec )
                    raise UnknownVisiDBSpec( 'ConnectionMgrVDBImpl.getSpecDictionary(): Cannot parse spec' )

                defn    = s3.group(1)
                vdbtype = s3.group(2)
                name    = s3.group(3)

                isExtractOrBinder = False

                if vdbtype == 'SpecLatticeExtract':
                    isExtractOrBinder = True

                s4 = re.search( 'SpecLatticeBinder\(SpecBoundName\(ContainerElementName\(\"\w*?\"\), NameTypeExtract', defn )
                if s4:
                    isExtractOrBinder = True

                if not isExtractOrBinder:
                    if name not in specs:
                        specs[name] = (vdbtype, defn, singlespec)

                if remainingspec == '':
                    break
                else:
                    specs_maude = remainingspec[2:]

        return specs


    def setSpec(self, obj_name, SpecLatticeNamedElements):

        url = self._url + '/DLRestService/InstallVisiDBStructureFile_Sync'

        vfile  = '<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<VisiDBStructures appName=\"\">\n  <workspaces>\n    <workspace name=\"Workspace\">\n      <specs>\n'
        vfile += SpecLatticeNamedElements
        vfile += '\n      </specs>\n    </workspace>\n  </workspaces>\n</VisiDBStructures>'

        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['value'] = vfile

        if self.isVerbose():
            print self.getType() +': Setting visiDB Spec for object \'{0}\'...'.format( obj_name ),

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )

        if response.status_code != 200:
            if self.isVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.SetSpec(): POST request returned code {0}'.format(response.status_code) )

        status = None
        errmsg = ''
        try:
            status = response.json()['Status']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.isVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if status == 5:
            if errmsg == 'Collection was modified; enumeration operation may not execute.':
                raise DataLoaderError( errmsg )
            else:
                raise MaudeStringError( errmsg )

        if status != 3:
            if self.isVerbose():
                print 'Failed to set spec \'{0}\''.format( obj_name )
                print errmsg
            raise TenantNotFoundAtURL( 'ConnectionMgrVDBImpl.SetSpec(): {0}'.format(payload['tenantName']) )

        value  = response.json()['Value']
        status2 = value[0]
        info2   = value[1]

        if status2['Value'] != 'Succeed':
            if self.isVerbose():
                print 'Failed to set spec \'{0}\''.format( obj_name )
                print info2['Value']
            raise DataLoaderError( 'ConnectionMgrVDBImpl.SetSpec(): {0}'.format(payload['tenantName']) )

        if self.isVerbose():
            print 'Success'


    def getQueryStatus(self, query_handle):

        url = self._url + '/DLQueryService/GetQueryStatus'

        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['queryHandle'] = query_handle

        if self.isVerbose():
            print self.getType() +': Getting query status for handle \'{0}\'...'.format( payload['queryHandle'] ),

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )

        if response.status_code != 200:
            if self.isVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.GetQueryStatus(): POST request returned code {0}'.format(response.status_code) )

        success = False
        errmsg = ''
        try:
            success = response.json()['Success']
            errmsg = response.json()['ErrorMessage']
            proginfo = response.json()['ProgressInfo']
        except ValueError as e:
            if self.isVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if not success or proginfo is None:
            if self.isVerbose():
                print 'Failed to get query status for handle \'{0}\''.format( payload['queryHandle'] )
                print errmsg
            raise UnknownVisiDBSpec( 'ConnectionMgrVDBImpl.getQueryStatus(): Query could not compile or execute in visiDB with handle {0}'.format(payload['queryHandle']) )

        if self.isVerbose():
            print 'Success'

        status   = response.json()['Status']
        progress = response.json()['Progress']

        if status == 3:
            return 'Completed'
        else:
            if proginfo == '':
                return 'Requested'
            elif proginfo[:9] == 'Compiling':
                return 'Compiling'
            elif proginfo[:13] == 'Executing cmd':
                return 'Executing ({0}%)'.format( progress )

        return 'Unknown'


    def getQueryResults( self, query_handle, start_row, row_count ):

        url = self._url + '/DLQueryService/GetQueryResultData'

        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['queryHandle'] = query_handle
        payload['startRow'] = start_row
        payload['rowCount'] = row_count

        if self.isVerbose():
            print self.getType() +': Getting query results for handle \'{0}\'...'.format( payload['queryHandle'] ),

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )

        if response.status_code != 200:
            if self.isVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.GetQueryResults(): POST request returned code {0}'.format(response.status_code) )

        success = False
        errmsg = ''
        try:
            success = response.json()['Success']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.isVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if not success:
            if self.isVerbose():
                print 'Failed to get query results for handle \'{0}\''.format( payload['queryHandle'] )
                print errmsg
            raise UnknownVisiDBSpec( 'ConnectionMgrVDBImpl.GetQueryResults(): {0}'.format(payload['queryHandle']) )

        if self.isVerbose():
            print 'Success'

        col_names     = []
        col_datatypes = []

        for c in response.json()['Columns']:
            col_names.append( c['ColumnName'] )
            col_datatypes.append( c['DataType'] )

        remaining_rows = response.json()['RemainingRows']

        rows = []

        datatable = etree.fromstring(self.__xmlunescape(response.json()['DataTable']).encode('ascii', 'xmlcharrefreplace'))
        de = datatable.find(".//DocumentElement")
        if de is not None:
            for row_xml in de:
                cols_list = []
                col_names_iter = iter(col_names)
                col_datatypes_iter = iter(col_datatypes)
                row_xml_iter = iter(row_xml)
                processing_cols = True
                while(True):

                    try:
                        name = col_names_iter.next()
                        datatype = col_datatypes_iter.next()
                    except StopIteration:
                        break

                    try:
                        col_xml = row_xml_iter.next()
                        tagname = col_xml.tag.replace('_x0020_',' ')
                    except StopIteration:
                        tagname = ''

                    while( name != tagname ):
                        cols_list.append( None )
                        try:
                            name = col_names_iter.next()
                            datatype = col_datatypes_iter.next()
                        except StopIteration:
                            processing_cols = False
                            break

                    if not processing_cols:
                        break

                    value = col_xml.text
                    if datatype == 0:
                        value = bool(col_xml.text)
                    elif datatype == 3:
                        value = int(col_xml.text)
                    elif datatype == 5:
                        value = float(col_xml.text)
                    elif datatype == 10:
                        value = datetime.datetime.strptime(col_xml.text[:19],'%Y-%m-%dT%H:%M:%S')
                    cols_list.append( value )
                rows.append( tuple(cols_list) )

        return (col_names, col_datatypes, remaining_rows, tuple(rows))


    def updateDataProvider(self, dataProviderName, dataSourceType, tryConnect, **kwargs):

        ## The available keys for each dataSourceType are:
        ##
        ## SQL: ServerName, Authentication, User, Password, Database, Schema
        ## SFDC: URL, User, Password, SecurityToken, Version
        ## Eloqua: URL, Company, Username, Password, EntityType
        ## Marketo: URL, UserID, EncryptionKey
        ## OracleCRM: URL, UserSignInID, Password
        ## Clarizen: URL, User, Password

        url = self._url + '/DLRestService/UpdateDataProvider'

        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['dataProviderName'] = dataProviderName
        payload['dataSourceType'] = dataSourceType
        payload['tryConnect'] = tryConnect

        values = []
        for k in kwargs.keys():
            v = {}
            v['Key'] = k
            v['Value'] = kwargs[k]
            values.append( v )

        payload['values'] = values

        if self.isVerbose():
            print self.getType() +': Updating DataProvider \'{0}\'...'.format( payload['dataProviderName'] ),

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )

        if response.status_code != 200:
            if self.isVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.UpdateDataProvider(): POST request returned code {0}'.format(response.status_code) )

        success = False
        errmsg = ''
        try:
            success = response.json()['Success']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.isVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if not success:
            if self.isVerbose():
                print 'Failed to update DataProvider \'{0}\''.format( payload['dataProviderName'] )
                print errmsg
            raise UnknownDataLoaderObject( 'ConnectionMgrVDBImpl.UpdateDataProvider(): {0}'.format(payload['dataProviderName']) )

        if self.isVerbose():
            print 'Success'


    def executeGroup(self, groupName, userEmail):

        url = self._url + '/DLRestService/ExecuteGroup'

        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['groupName'] = groupName
        payload['email'] = userEmail
        payload['state'] = 'launch'
        payload['sync'] = 'false'

        if self.isVerbose():
            print self.getType() +': Executing Load Group \'{0}\'...'.format( payload['groupName'] ),

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )

        if response.status_code != 200:
            if self.isVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.executeGroup(): POST request returned code {0}'.format(response.status_code) )

        status = None
        errmsg = ''
        try:
            status = response.json()['Status']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.isVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError(e)

        if status == 5:
            if self.isVerbose():
                print 'Failed to execute Load Group \'{0}\': status={1}'.format(payload['groupName'], status)
                print errmsg
            raise DataLoaderError('ConnectionMgrVDBImpl.executeGroup(): {0}'.format(payload['tenantName']))

        if status != 3:
            if self.isVerbose():
                print 'Failed to execute Load Group \'{0}\': status={1}'.format(payload['groupName'], status)
                print errmsg
            raise TenantNotFoundAtURL('ConnectionMgrVDBImpl.executeGroup(): {0}'.format(payload['tenantName']))

        valuedict  = response.json()['Value'][0]
        launchid = valuedict['Value']

        if self.isVerbose():
            print 'Success'

        return launchid


    def installDLConfigFile(self, config):

        url = self._url + '/DLRestService/InstallConfigFile_Sync'

        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['value'] = config

        if self.isVerbose():
            print self.getType() +': Setting DataLoader Config File...',

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )

        if response.status_code != 200:
            if self.isVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.installDLConfigFile(): POST request returned code {0}'.format(response.status_code) )

        status = None
        errmsg = ''
        try:
            status = response.json()['Status']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.isVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if status == 5:
            raise XMLStringError( errmsg )

        if status != 3:
            if self.isVerbose():
                print 'Failed to set config'
                print errmsg
            raise TenantNotFoundAtURL( 'ConnectionMgrVDBImpl.installDLConfigFile(): {0}'.format(payload['tenantName']) )

        value  = response.json()['Value']
        status2 = value[0]
        info2   = value[1]

        if status2['Value'] != 'Succeed':
            if self.isVerbose():
                print 'Failed to set config'
                print info2['Value']
            raise DataLoaderError( 'ConnectionMgrVDBImpl.installDLConfigFile(): {0}'.format(payload['tenantName']) )

        if self.isVerbose():
            print 'Success'


    def getDLTenantSettings(self):

        url = self._url + '/DLRestService/GetDLTenantSettings'

        payload = {}
        payload['tenantName'] = self._tenant_name

        if self.isVerbose():
            print self.getType() +': Get DataLoader Tenant Settings...',

        try:
            response = requests.post(url, json=payload, headers=self._headers, verify=self._verify)
        except requests.exceptions.ConnectionError as e:
            raise EndpointError(e)

        if response.status_code != 200:
            if self.isVerbose():
                print 'HTTP POST request failed'
            raise HTTPError('ConnectionMgrVDBImpl.getDLTenantSettings(): POST request returned code {0}'.format(response.status_code))

        status = None
        errmsg = ''
        try:
            status = response.json()['Status']
            errmsg = response.json()['ErrorMessage']

        except ValueError as e:
            if self.isVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError(e)

        if status == 5:
            raise VisiDBNotFoundOnServer(errmsg)

        if status != 3:
            if self.isVerbose():
                print 'Failed to get DL Tenant Settings'
                print errmsg
            raise TenantNotFoundAtURL('ConnectionMgrVDBImpl.getDLTenantSettings(): {0}'.format(payload['tenantName']))

        if self.isVerbose():
            print 'Success'

        return(response.json()["Value"])


    def setMonitorTenant(self, enable):

        url = self._url + '/DLRestService/UpdateDLTenantSettings'

        payload = {}
        payload['tenantName'] = self._tenant_name
        if enable:
            payload['values'] = [{'Key':'MonitorTenant', 'Value':'1'}]
        else:
            payload['values'] = [{'Key':'MonitorTenant', 'Value':'0'}]

        if self.isVerbose():
            print self.getType() +': Set DataLoader Monitor Tenant...',

        try:
            response = requests.post(url, json=payload, headers=self._headers, verify=self._verify)
        except requests.exceptions.ConnectionError as e:
            raise EndpointError(e)

        if response.status_code != 200:
            if self.isVerbose():
                print 'HTTP POST request failed'
            raise HTTPError('ConnectionMgrVDBImpl.setMonitorTenant(): POST request returned code {0}'.format(response.status_code))

        status = None
        errmsg = ''
        try:
            status = response.json()['Status']
            errmsg = response.json()['ErrorMessage']

        except ValueError as e:
            if self.isVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError(e)

        if status != 3:
            if self.isVerbose():
                print 'Failed to set DL Monitor Tenant'
                print errmsg
            raise TenantNotFoundAtURL('ConnectionMgrVDBImpl.setMonitorTenant(): {0}'.format(payload['tenantName']))

        if self.isVerbose():
            print 'Success'


    def getLoadGroupStatus(self, groupName):

        url = self._url + '/DLRestService/GetLoadGroupStatus'

        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['groupName'] = groupName

        if self.isVerbose():
            print self.getType() +': Get DataLoader Load Group Status...',

        try:
            response = requests.post(url, json=payload, headers=self._headers, verify=self._verify)
        except requests.exceptions.ConnectionError as e:
            raise EndpointError(e)

        if response.status_code != 200:
            if self.isVerbose():
                print 'HTTP POST request failed'
            raise HTTPError('ConnectionMgrVDBImpl.getLoadGroupStatus(): POST request returned code {0}'.format(response.status_code))

        status = None
        errmsg = ''
        try:
            status = response.json()['Status']
            errmsg = response.json()['ErrorMessage']

        except ValueError as e:
            if self.isVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError(e)

        if status != 3:
            if self.isVerbose():
                print 'Failed to get DL Load Group Status'
                print errmsg
            raise TenantNotFoundAtURL('ConnectionMgrVDBImpl.getLoadGroupStatus(): {0}'.format(payload['tenantName']))

        if self.isVerbose():
            print 'Success'

        values = {}
        for d in response.json()['Value']:
            k = d['Key']
            v = d['Value']
            values[k] = v

        return values


    def getLaunchStatus(self, launchid):

        url = self._url + '/DLRestService/GetLaunchStatus'

        payload = {}
        payload['launchId'] = launchid

        if self.isVerbose():
            print self.getType() +': Get DataLoader Launch Status...',

        try:
            response = requests.post(url, json=payload, headers=self._headers, verify=self._verify)
        except requests.exceptions.ConnectionError as e:
            raise EndpointError(e)

        if response.status_code != 200:
            if self.isVerbose():
                print 'HTTP POST request failed'
            raise HTTPError('ConnectionMgrVDBImpl.getLaunchStatus(): POST request returned code {0}'.format(response.status_code))

        status = None
        errmsg = ''
        try:
            status = response.json()['Status']
            errmsg = response.json()['ErrorMessage']

        except ValueError as e:
            if self.isVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError(e)

        if status != 3:
            if self.isVerbose():
                print 'Failed to get DL Launch Status'
                print errmsg
            raise TenantNotFoundAtURL('ConnectionMgrVDBImpl.getLaunchStatus(): {0}'.format(payload['tenantName']))

        if self.isVerbose():
            print 'Success'

        values = {}
        for d in response.json()['Value']:
            k = d['Key']
            v = d['Value']
            values[k] = v

        return values


    @classmethod
    def __getURLForTenant(cls, tenant):

        theurl = None

        pathname = os.path.dirname(__file__)
        pathname = os.path.join( pathname, '..', '..', 'resources', 'tenant_url.csv' )

        with open( pathname ) as fh:
            for line in fh:
                (ten,url) = line.strip().split(',')
                if ten == tenant:
                    theurl = url
                    break

        if theurl is None:
            raise TenantNotMappedToURL( tenant )

        return theurl


    @classmethod
    def __xmlfixup(cls, m):
        text = m.group(0)
        code = int(text[3:-1], 16)
        if code < 32:
            return u''

        return text # leave as is


    @classmethod
    def __xmlunescape(cls, text):
        return re.sub("&#x\w+;", cls.__xmlfixup, text)

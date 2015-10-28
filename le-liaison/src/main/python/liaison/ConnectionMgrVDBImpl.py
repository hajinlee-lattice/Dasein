
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

import os, datetime
from .exceptions import HTTPError, EndpointError, TenantNotMappedToURL, TenantNotFoundAtURL, DataLoaderError
from .exceptions import UnknownVisiDBSpec, UnknownDataLoaderObject, MaudeStringError, XMLStringError
from .ConnectionMgr import ConnectionMgr
from .TableVDBImpl import TableVDBImpl
from .QueryVDBImpl import QueryVDBImpl
from .NamedExpressionVDBImpl import NamedExpressionVDBImpl
from .QueryResultVDBImpl import QueryResultVDBImpl
from .LoadGroupMgrImpl import LoadGroupMgrImpl


class ConnectionMgrVDBImpl( ConnectionMgr ):

    def __init__( self, type, tenant_name, dataloader_url = None, verify = True, verbose = False ):
        
        super( ConnectionMgrVDBImpl, self ).__init__( type, verbose )

        self._headers = {}
        self._headers['MagicAuthentication'] = 'Security through obscurity!'
        self._headers['Content-Type']        = 'application/json; charset=utf-8'

        if dataloader_url is None:
            dataloader_url = self.GetURLForTenant( tenant_name )
        
        self._url         = dataloader_url
        self._tenant_name = tenant_name
        self._verify      = verify
        if dataloader_url in ['https://10.41.1.187:8080/','http://10.41.1.207:8081/']:
            self._verify = False


    def GetTable( self, table_name ):

        table_name_import = table_name + '_Import'
        
        itable_spec = self.GetSpec( table_name_import )
        stable_spec = self.GetSpec( table_name )

        table = TableVDBImpl.InitFromDefn( table_name, stable_spec, itable_spec )

        return table
    

    def SetTable( self, table ):

        self.SetSpec( table.Name(), table.SpecLatticeNamedElements() )


    def GetNamedExpression( self, expression_name ):

        spec = self.GetSpec( expression_name )

        expression = NamedExpressionVDBImpl.InitFromDefn( expression_name, spec )

        return expression
    

    def SetNamedExpression( self, expression ):

        self.SetSpec( expression.Name(), expression.SpecLatticeNamedElements() )


    def GetQuery( self, query_name ):

        spec = self.GetSpec( query_name )

        query = QueryVDBImpl.initFromDefn( query_name, spec )

        return query
    

    def GetMetadata( self, query_name ):

        url = self._url + '/DLRestService/GetQueryMetadataColumns'
        
        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['queryName'] = query_name

        if self.IsVerbose():
            print self.Type() +': Getting visiDB Metadata for Query \'{0}\'...'.format( payload['queryName'] ),

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )
        
        if response.status_code != 200:
            if self.IsVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.GetSpec(): POST request returned code {0}'.format(response.status_code) )

        status = None
        errmsg = ''
        try:
            status = response.json()['Status']
            errmsg = response.json()['ErrorMessage']
            mdraw  = response.json()['Metadata']
        except ValueError as e:
            if self.IsVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if status != 3:
            if self.IsVerbose():
                print 'Failed to get metadata for query \'{0}\''.format( payload['queryName'] )
                print errmsg
            raise TenantNotFoundAtURL( 'ConnectionMgrVDBImpl.GetMetadata(): {0}'.format(payload['tenantName']) )

        if self.IsVerbose():
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
            metadata['DataType']        = None
            if col_data['Extensions'] is not None:
                for ext in col_data['Extensions']:
                    k = ext['Key']
                    v = ext['Value']
                    if k not in ['Category','DataType']:
                        continue
                    metadata[k]             = v
            metadata['DataSource']      = None
            if col_data['DataSource'] is not None and len(col_data['DataSource']) > 0:
                metadata['DataSource']     = col_data['DataSource'][-1]
            metadata['ApprovedUsage']   = None
            if col_data['ApprovedUsage'] is not None and len(col_data['ApprovedUsage']) > 0:
                metadata['ApprovedUsage'] = col_data['ApprovedUsage'][-1]
            metadata['StatisticalType'] = col_data['StatisticalType']

            columns[ col_data['ColumnName'] ] = metadata

        return columns


    def SetQuery( self, query ):

        self.SetSpec( query.Name(), query.SpecLatticeNamedElements() )


    def ExecuteQuery( self, query_name ):

        url = self._url + '/DLQueryService/RunQuery'

        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['queryName'] = query_name

        if self.IsVerbose():
            print self.Type() +': Executing Query \'{0}\'...'.format( payload['queryName'] ),

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )

        if response.status_code != 200:
            if self.IsVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.ExecuteQuery(): POST request returned code {0}'.format(response.status_code) )

        success = False
        errmsg = ''
        try:
            success = response.json()['Success']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.IsVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if not success:
            if self.IsVerbose():
                print 'Failed to execute query \'{0}\''.format( payload['queryName'] )
                print errmsg
            raise UnknownVisiDBSpec( 'ConnectionMgrVDBImpl.ExecuteQuery(): {0}'.format(payload['queryName']) )
        
        if self.IsVerbose():
            print 'Success'

        return QueryResultVDBImpl( self, response.json()['QueryHandle'] )


    def GetLoadGroupMgr( self ):

        url = self._url + '/DLRestService/DownloadConfigFile'
        
        payload = {}
        payload['tenantName'] = self._tenant_name

        if self.IsVerbose():
            print self.Type() +': Getting DataLoader Config File...',

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )
        
        if response.status_code != 200:
            if self.IsVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.GetLoadGroupMgr(): POST request returned code {0}'.format(response.status_code) )

        status = None
        errmsg = ''
        try:
            status = response.json()['Status']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.IsVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if status != 3:
            if self.IsVerbose():
                print 'Failed to get config file'
                print errmsg
            raise TenantNotFoundAtURL( 'ConnectionMgrVDBImpl.GetLoadGroupMgr(): {0}'.format(payload['tenantName']) )
        
        valuedict  = response.json()['Value'][0]
        configfile = valuedict['Value'].encode('ascii', 'xmlcharrefreplace')
        
        lgm = LoadGroupMgrImpl( self, configfile )

        if self.IsVerbose():
            print 'Success'

        return lgm

 
    def GetSpec( self, spec_name ):
        
        url = self._url + '/DLRestService/GetSpecDetails'
        
        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['specName'] = spec_name

        if self.IsVerbose():
            print self.Type() +': Getting visiDB Spec \'{0}\'...'.format( payload['specName'] ),

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )
        
        if response.status_code != 200:
            if self.IsVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.GetSpec(): POST request returned code {0}'.format(response.status_code) )

        success = False
        errmsg = ''
        try:
            success = response.json()['Success']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.IsVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if not success:
            if self.IsVerbose():
                print 'Failed to get spec \'{0}\''.format( payload['specName'] )
                print errmsg
            if errmsg == 'Tenant \'{0}\' does not exist.'.format( payload['tenantName'] ):
                raise TenantNotFoundAtURL( 'ConnectionMgrVDBImpl.GetSpec(): {0}'.format(payload['tenantName']) )
            else:
                raise UnknownVisiDBSpec( 'ConnectionMgrVDBImpl.GetSpec(): {0}'.format(payload['specName']) )
        
        if self.IsVerbose():
            print 'Success'

        return response.json()['SpecDetails']


    def GetAllSpecs( self ):
        
        url = self._url + '/DLRestService/DownloadVisiDBStructureFile'
        
        payload = {}
        payload['tenantName'] = self._tenant_name

        if self.IsVerbose():
            print self.Type() +': Getting All visiDB Specs...',

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )
        
        if response.status_code != 200:
            if self.IsVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.GetAllSpecs(): POST request returned code {0}'.format(response.status_code) )

        status = None
        errmsg = ''
        try:
            status = response.json()['Status']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.IsVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if status != 3:
            if self.IsVerbose():
                print 'Failed to get all specs'
                print errmsg
            raise TenantNotFoundAtURL( 'ConnectionMgrVDBImpl.GetAllSpecs(): {0}'.format(payload['tenantName']) )
        
        valuedict  = response.json()['Value'][0]
        specfile = valuedict['Value'].encode('ascii', 'xmlcharrefreplace')
        
        specxml = etree.fromstring(specfile)
        specs =  specxml.find(".//specs")
        if specs is None:
            raise UnknownVisiDBSpec( 'No specs returned' )

        if self.IsVerbose():
            print 'Success'

        return specs.text


    def SetSpec( self, obj_name, SpecLatticeNamedElements ):

        url = self._url + '/DLRestService/InstallVisiDBStructureFile_Sync'

        vfile  = '<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<VisiDBStructures appName=\"\">\n  <workspaces>\n    <workspace name=\"Workspace\">\n      <specs>\n'
        vfile += SpecLatticeNamedElements
        vfile += '\n      </specs>\n    </workspace>\n  </workspaces>\n</VisiDBStructures>'

        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['value'] = vfile

        if self.IsVerbose():
            print self.Type() +': Setting visiDB Spec for object \'{0}\'...'.format( obj_name ),

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )
        
        if response.status_code != 200:
            if self.IsVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.SetSpec(): POST request returned code {0}'.format(response.status_code) )

        status = None
        errmsg = ''
        try:
            status = response.json()['Status']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.IsVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if status == 5:
            if errmsg == 'Collection was modified; enumeration operation may not execute.':
                raise DataLoaderError( errmsg )
            else:
                raise MaudeStringError( errmsg )

        if status != 3:
            if self.IsVerbose():
                print 'Failed to set spec \'{0}\''.format( obj_name )
                print errmsg
            raise TenantNotFoundAtURL( 'ConnectionMgrVDBImpl.SetSpec(): {0}'.format(payload['tenantName']) )
        
        value  = response.json()['Value']
        status2 = value[0]
        info2   = value[1]

        if status2['Value'] != 'Succeed':
            if self.IsVerbose():
                print 'Failed to set spec \'{0}\''.format( obj_name )
                print info2['Value']
            raise DataLoaderError( 'ConnectionMgrVDBImpl.SetSpec(): {0}'.format(payload['tenantName']) )

        if self.IsVerbose():
            print 'Success'


    def GetQueryStatus( self, query_handle ):

        url = self._url + '/DLQueryService/GetQueryStatus'

        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['queryHandle'] = query_handle

        if self.IsVerbose():
            print self.Type() +': Getting query status for handle \'{0}\'...'.format( payload['queryHandle'] ),

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )

        if response.status_code != 200:
            if self.IsVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.GetQueryStatus(): POST request returned code {0}'.format(response.status_code) )

        success = False
        errmsg = ''
        try:
            success = response.json()['Success']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.IsVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if not success:
            if self.IsVerbose():
                print 'Failed to get query status for handle \'{0}\''.format( payload['queryHandle'] )
                print errmsg
            raise UnknownVisiDBSpec( 'ConnectionMgrVDBImpl.GetQueryStatus(): {0}'.format(payload['queryHandle']) )
        
        if self.IsVerbose():
            print 'Success'

        status   = response.json()['Status']
        progress = response.json()['Progress']
        proginfo = response.json()['ProgressInfo']

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


    def GetQueryResults( self, query_handle, start_row, row_count ):

        url = self._url + '/DLQueryService/GetQueryResultData'

        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['queryHandle'] = query_handle
        payload['startRow'] = start_row
        payload['rowCount'] = row_count

        if self.IsVerbose():
            print self.Type() +': Getting query results for handle \'{0}\'...'.format( payload['queryHandle'] ),

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )

        if response.status_code != 200:
            if self.IsVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.GetQueryResults(): POST request returned code {0}'.format(response.status_code) )

        success = False
        errmsg = ''
        try:
            success = response.json()['Success']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.IsVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if not success:
            if self.IsVerbose():
                print 'Failed to get query results for handle \'{0}\''.format( payload['queryHandle'] )
                print errmsg
            raise UnknownVisiDBSpec( 'ConnectionMgrVDBImpl.GetQueryResults(): {0}'.format(payload['queryHandle']) )
        
        if self.IsVerbose():
            print 'Success'

        col_names     = []
        col_datatypes = []

        for c in response.json()['Columns']:
            col_names.append( c['ColumnName'] )
            col_datatypes.append( c['DataType'] )

        remaining_rows = response.json()['RemainingRows']

        rows = []
        datatable = etree.fromstring( response.json()['DataTable'].encode('ascii', 'xmlcharrefreplace') )
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

        return ( col_names, col_datatypes, remaining_rows, tuple(rows) )


    def UpdateDataProvider( self, dataProviderName, dataSourceType, tryConnect, **kwargs ):

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

        if self.IsVerbose():
            print self.Type() +': Updating DataProvider \'{0}\'...'.format( payload['dataProviderName'] ),

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )

        if response.status_code != 200:
            if self.IsVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.UpdateDataProvider(): POST request returned code {0}'.format(response.status_code) )

        success = False
        errmsg = ''
        try:
            success = response.json()['Success']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.IsVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if not success:
            if self.IsVerbose():
                print 'Failed to update DataProvider \'{0}\''.format( payload['dataProviderName'] )
                print errmsg
            raise UnknownDataLoaderObject( 'ConnectionMgrVDBImpl.UpdateDataProvider(): {0}'.format(payload['dataProviderName']) )
        
        if self.IsVerbose():
            print 'Success'


    def executeGroup( self, groupName, userEmail ):
        
        url = self._url + '/DLRestService/ExecuteGroup'
        
        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['groupName'] = groupName
        payload['email'] = userEmail
        payload['state'] = 'launch'
        payload['sync'] = 'false'

        if self.IsVerbose():
            print self.Type() +': Executing Load Group \'{0}\'...'.format( payload['groupName'] ),

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )
        
        if response.status_code != 200:
            if self.IsVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.executeGroup(): POST request returned code {0}'.format(response.status_code) )

        status = None
        errmsg = ''
        try:
            status = response.json()['Status']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.IsVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if status != 3:
            if self.IsVerbose():
                print 'Failed to execute Load Group \'{0}\''.format( payload['groupName'] )
                print errmsg
            raise TenantNotFoundAtURL( 'ConnectionMgrVDBImpl.executeGroup(): {0}'.format(payload['tenantName']) )
        
        valuedict  = response.json()['Value'][0]
        launchid = valuedict['Value']
        
        if self.IsVerbose():
            print 'Success'

        return launchid


    def installDLConfigFile( self, config ):

        url = self._url + '/DLRestService/InstallConfigFile_Sync'

        payload = {}
        payload['tenantName'] = self._tenant_name
        payload['value'] = config

        if self.IsVerbose():
            print self.Type() +': Setting DataLoader Config File...',

        try:
            response = requests.post( url, json=payload, headers=self._headers, verify=self._verify )
        except requests.exceptions.ConnectionError as e:
            raise EndpointError( e )
        
        if response.status_code != 200:
            if self.IsVerbose():
                print 'HTTP POST request failed'
            raise HTTPError( 'ConnectionMgrVDBImpl.installDLConfigFile(): POST request returned code {0}'.format(response.status_code) )

        status = None
        errmsg = ''
        try:
            status = response.json()['Status']
            errmsg = response.json()['ErrorMessage']
        except ValueError as e:
            if self.IsVerbose():
                print 'HTTP Endpoint did not return the expected response'
            raise EndpointError( e )

        if status == 5:
            raise XMLStringError( errmsg )

        if status != 3:
            if self.IsVerbose():
                print 'Failed to set config'
                print errmsg
            raise TenantNotFoundAtURL( 'ConnectionMgrVDBImpl.installDLConfigFile(): {0}'.format(payload['tenantName']) )
        
        value  = response.json()['Value']
        status2 = value[0]
        info2   = value[1]

        if status2['Value'] != 'Succeed':
            if self.IsVerbose():
                print 'Failed to set config'
                print info2['Value']
            raise DataLoaderError( 'ConnectionMgrVDBImpl.installDLConfigFile(): {0}'.format(payload['tenantName']) )

        if self.IsVerbose():
            print 'Success'


    @classmethod
    def GetURLForTenant( cls, tenant ):

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


#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import re
from .exceptions import MaudeStringError
from .QueryColumn import QueryColumn
from .ExpressionVDBImpl import *


class QueryColumnVDBImpl( QueryColumn ):

## This is the default constructor

    def __init__( self, name, expression, approved_usage = None, display_name = None,
                                category = None, statistical_type = None, tags = None,
                                fundamental_type = None, description = None,
                                display_discretization = None ):

        self.InitFromValues( name, expression, approved_usage, display_name, category,
                                                 statistical_type, tags, fundamental_type,
                                                 description, display_discretization )


## This is the pythonic way to create multiple constructors.  The call is
## c = QueryColumnVDBImpl.InitFromDefn( defn )

    @classmethod
    def InitFromDefn( cls, defn ):

        name                   = None
        expression             = None

        md = {}
        md['ApprovedUsage']    = None
        md['DisplayName']      = None
        md['Category']         = None
        md['StatisticalType']  = None
        md['Tags']             = None
        md['FundamentalType']  = None
        md['Description']      = None
        md['DisplayDiscretizationStrategy'] = None

        c_no_metadata = re.search( '^SpecQueryNamedFunctionExpression\(ContainerElementName\(\"(\w*)\"\), (LatticeFunction.*)\)$', defn )

        if c_no_metadata:
            name = c_no_metadata.group(1)
            expression = ExpressionVDBImplFactory.Create( c_no_metadata.group(2) )

        c_fcnbndry = re.search( '^SpecQueryNamedFunctionEntityFunctionBoundary$', defn )

        if c_fcnbndry:
            name = 'EntityFunctionBoundary'
            expression = ExpressionVDBImplFactory.Create( 'SpecQueryNamedFunctionEntityFunctionBoundary' )
        
        c_has_metadata = re.search( '^SpecQueryNamedFunctionMetadata\(SpecQueryNamedFunctionExpression\(ContainerElementName\(\"(\w*)\"\), (LatticeFunction.*)\), SpecExtractDetails\(\((.*)\)\)\)$', defn )

        if c_has_metadata:
            name = c_has_metadata.group(1)
            expression = ExpressionVDBImplFactory.Create( c_has_metadata.group(2) )
            sed = c_has_metadata.group(3)
            if name == "Industry_Group":
                print defn

            while True:
                metadata_is_extracted = False
                
                for md_name in md:
                    md_found = re.search( '^SpecExtractDetail\(\"{0}\", \"(.*?)\"\)(, SpecExtractDetail\(.*|$)'.format(md_name), sed )
                    if md_found:
                        md[md_name] = md_found.group(1).replace('\\"','"')
                        sed = md_found.group(2)[2:]
                        metadata_is_extracted = True
                        break

                    md_found = re.search( '^SpecExtractDetail\(\"{0}\", StringList\(.*?\"(.*?)\"\)\)(, SpecExtractDetail\(.*|$)'.format(md_name), sed )
                    if md_found:
                        md[md_name] = md_found.group(1).replace('\\"','"')
                        sed = md_found.group(2)[2:]
                        metadata_is_extracted = True
                        break

                if not metadata_is_extracted:
                    raise MaudeStringError( sed )

                if sed == '':
                    break

        if expression is None:
            raise MaudeStringError( defn )
        
        return cls( name, expression, md['ApprovedUsage'], md['DisplayName'], md['Category'],
                                md['StatisticalType'], md['Tags'], md['FundamentalType'], md['Description'],
                                md['DisplayDiscretizationStrategy'] )


    def Definition( self ):

        if self.Name() == 'EntityFunctionBoundary':
            return 'SpecQueryNamedFunctionEntityFunctionBoundary'

        mdsep = ''
        md = ''
        if self.DisplayName() is not None:
            md += mdsep + 'SpecExtractDetail("DisplayName", "{0}")'.format( self.DisplayName().replace('"','\\"') )
            mdsep = ', '
        if self.Description() is not None:
            md += mdsep + 'SpecExtractDetail("Description", "{0}")'.format( self.Description().replace('"','\\"') )
            mdsep = ', '
        if self.ApprovedUsage() is not None:
            md += mdsep + 'SpecExtractDetail("ApprovedUsage", "{0}")'.format( self.ApprovedUsage().replace('"','\\"') )
            mdsep = ', '
        if self.FundamentalType() is not None:
            md += mdsep + 'SpecExtractDetail("FundamentalType", "{0}")'.format( self.FundamentalType().replace('"','\\"') )
            mdsep = ', '
        if self.StatisticalType() is not None:
            md += mdsep + 'SpecExtractDetail("StatisticalType", "{0}")'.format( self.StatisticalType().replace('"','\\"') )
            mdsep = ', '
        if self.Tags() is not None:
            md += mdsep + 'SpecExtractDetail("Tags", "{0}")'.format( self.Tags().replace('"','\\"') )
            mdsep = ', '
        if self.DisplayDiscretization() is not None:
            md += mdsep + 'SpecExtractDetail("DisplayDiscretizationStrategy", "{0}")'.format( self.DisplayDiscretization().replace('"','\\"') )
            mdsep = ', '
        if self.Category() is not None:
            md += mdsep + 'SpecExtractDetail("Category", "{0}")'.format( self.Category().replace('"','\\"') )
            mdsep = ', '

        sqnf =  'SpecQueryNamedFunctionExpression('
        sqnf +=   'ContainerElementName("{0}")'.format( self.Name() )
        sqnf += ', '+ self.Expression().Definition()
        sqnf += ')'

        if md == '':
            return sqnf

        sqnfm =  'SpecQueryNamedFunctionMetadata('
        sqnfm += sqnf
        sqnfm += ', SpecExtractDetails(('
        sqnfm += md
        sqnfm +=   '))'
        sqnfm += ')'
        
        return sqnfm

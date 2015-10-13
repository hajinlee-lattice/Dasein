
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import re
from .exceptions import MaudeStringError, UnknownVisiDBType
from . import types_vdb
from .TableColumn import TableColumn


class TableColumnVDBImpl( TableColumn ):

## This is the default constructor

    def __init__( self, name, tablename, datatype, other_specs = None ):

        self.InitFromValues( name, tablename, datatype, other_specs )


## This is the pythonic way to create multiple constructors.  The call is
## t = TableColumnVDBImpl.InitFromDefn( defn )

    @classmethod
    def InitFromDefn( cls, defn ):

        c = re.search( '^SpecLatticeSourceTableColumn\(LatticeFunctionIdentifier\(ContainerElementNameTableQualifiedName\(LatticeSourceTableIdentifier\(ContainerElementName\(\"(\w*)\"\)\), ContainerElementName\(\"(\w*)\"\)\)\), (.*?), ((\w*)\(.*\))\)$', defn )

        ## Note: the datatype extracted from the spec has 'DataType' as the first 8 characters at the beginning.  We strip that out to initialize the datatype variable.
        if c:
            return cls( c.group(2), c.group(1), c.group(3)[8:], c.group(4) )
        else:
            raise MaudeStringError( defn )


    def OtherSpecs( self ):
        return self._other_specs


    def SetOtherSpecs( self, os ):
        self._other_specs = os
        return self._other_specs


    def Definition( self ):

        defn =  'SpecLatticeSourceTableColumn('
        defn +=   'LatticeFunctionIdentifier('
        defn +=     'ContainerElementNameTableQualifiedName('
        defn +=       'LatticeSourceTableIdentifier(ContainerElementName(\"{0}\"))'.format( self.TableName() )
        defn +=     ', ContainerElementName(\"{0}\")'.format( self.Name() )
        defn +=     ')'
        defn +=   ')'
        defn += ', {0}'.format( 'DataType' + self.Datatype() )
        defn += ', {0}'.format( self.OtherSpecs() )
        defn += ')'

        return defn


    def InitFromValues( self, name, tablename, datatype, other_specs = None ):

        if not types_vdb.IsStandardType( datatype ):
            raise UnknownVisiDBType( datatype )

        super( TableColumnVDBImpl, self ).InitFromValues( name, tablename, datatype )

        if other_specs is not None:
            self._other_specs = other_specs
        
        else:
            
            agg = types_vdb.AggregationForStandardType( datatype )

            self._other_specs =    'SpecFieldTypeMetric(FunctionAggregationOperator(\"{0}\"))'.format( agg )
            self._other_specs += ', SpecColumnContentContainerElementName(ContainerElementName(\"{0}\"))'.format( self.Name() )
            self._other_specs += ', SpecEndpointTypeNone'
            self._other_specs += ', SpecDefaultValueNull'
            self._other_specs += ', SpecKeyAggregation(SpecColumnAggregationRuleFunction(\"{0}\"))'.format( agg )
            self._other_specs += ', SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction(\"{0}\"))'.format( agg )


    




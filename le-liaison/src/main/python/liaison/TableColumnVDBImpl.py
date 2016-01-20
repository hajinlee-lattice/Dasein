
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import re
from .exceptions import MaudeStringError, UnknownVisiDBType
from . import types_vdb
from .TableColumn import TableColumn


class TableColumnVDBImpl(TableColumn):

## This is the default constructor

    def __init__(self, name, tablename, datatype, other_specs = None):

        self.initFromValues(name, tablename, datatype, other_specs)


## This is the pythonic way to create multiple constructors.  The call is
## t = TableColumnVDBImpl.InitFromDefn( defn )

    @classmethod
    def initFromDefn(cls, defn):

        c = re.search( '^SpecLatticeSourceTableColumn\(LatticeFunctionIdentifier\(ContainerElementNameTableQualifiedName\(LatticeSourceTableIdentifier\(ContainerElementName\(\"(\w*)\"\)\), ContainerElementName\(\"(.*?)\"\)\)\), (.*?), ((\w*).*\))\)$', defn )

        ## Note: the datatype extracted from the spec has 'DataType' as the first 8 characters at the beginning.  We strip that out to initialize the datatype variable.
        if c:
            return cls(c.group(2), c.group(1), c.group(3)[8:], c.group(4))
        else:
            raise MaudeStringError( defn )


    def getOtherSpecs(self):
        return self.other_specs_


    def setOtherSpecs(self, os):
        self.other_specs_ = os


    def definition( self ):

        defn =  'SpecLatticeSourceTableColumn('
        defn +=   'LatticeFunctionIdentifier('
        defn +=     'ContainerElementNameTableQualifiedName('
        defn +=       'LatticeSourceTableIdentifier(ContainerElementName(\"{0}\"))'.format( self.getTableName() )
        defn +=     ', ContainerElementName(\"{0}\")'.format( self.getName() )
        defn +=     ')'
        defn +=   ')'
        defn += ', {0}'.format( 'DataType' + self.getDatatype() )
        defn += ', {0}'.format( self.getOtherSpecs() )
        defn += ')'

        return defn


    def initFromValues(self, name, tablename, datatype, other_specs = None):

        if not types_vdb.IsStandardType( datatype ):
            raise UnknownVisiDBType( datatype )

        super(TableColumnVDBImpl, self).initFromValues(name, tablename, datatype)

        if other_specs is not None:
            self.other_specs_ = other_specs
        
        else:
            
            agg = types_vdb.AggregationForStandardType( datatype )

            self.other_specs_ =    'SpecFieldTypeMetric(FunctionAggregationOperator(\"{0}\"))'.format( agg )
            self.other_specs_ += ', SpecColumnContentContainerElementName(ContainerElementName(\"{0}\"))'.format( self.getName() )
            self.other_specs_ += ', SpecEndpointTypeNone'
            self.other_specs_ += ', SpecDefaultValueNull'
            self.other_specs_ += ', SpecKeyAggregation(SpecColumnAggregationRuleFunction(\"{0}\"))'.format( agg )
            self.other_specs_ += ', SpecEquivalenceAggregation(SpecColumnAggregationRuleFunction(\"{0}\"))'.format( agg )


    




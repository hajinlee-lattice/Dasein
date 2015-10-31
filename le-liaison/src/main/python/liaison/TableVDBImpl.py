
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import re
from .exceptions import MaudeStringError
from .Table import Table
from .TableColumnVDBImpl import TableColumnVDBImpl


class TableVDBImpl(Table):

## This is the default constructor

    def __init__(self, name, columns, srctable_specs = None, aggrule_spec = None, filter_spec = None):

        self.initFromValues(name, columns, srctable_specs, aggrule_spec, filter_spec)


## This is the pythonic way to create multiple constructors.  The call is
## t = TableVDBImpl.InitFromDefn( srctable_defn, imptable_defn )

    @classmethod
    def initFromDefn(cls, name, srctable_defn, imptable_defn):

        s1 = re.search( '^SpecLatticeSourceTable\(SpecLatticeSourceTableColumnSet\(\((.*)\)\), (SpecKeys(.*))\)$', srctable_defn )
        if not s1:
            raise MaudeStringError( srctable_defn )
        
        srctable_specs = s1.group(2)

        columns = []
        stcs = s1.group(1)

        if stcs != 'empty':
            ## Iterate on the comma-separated SpecLatticeSourceTableColumn specs
            while True:
                s2 = re.search( '^(SpecLatticeSourceTableColumn\(.*?\))(, SpecLatticeSourceTableColumn\(.*|$)', stcs )
                
                if not s2:
                    raise MaudeStringError( srctable_defn )

                c = TableColumnVDBImpl.initFromDefn( s2.group(1) )
                columns.append( c )

                if s2.group(2) == '':
                    break
                else:
                    ## Strip off the ', ' at the beginning
                    stcs = s2.group(2)[2:]

        i1 = re.search( '^SpecLatticeImportTable\((SpecSourceAggregationRule.*?), SpecColumnBindings', imptable_defn )
        if not i1:
            raise MaudeStringError( imptable_defn )

        aggrule_spec = i1.group(1)

        i2 = re.search( '^SpecLatticeImportTable\(.*, (SpecSourceFilter\w+)\)$', imptable_defn )
        if not i2:
            raise MaudeStringError( imptable_defn )

        filter_spec = i2.group(1)

        return cls(name, columns, srctable_specs, aggrule_spec, filter_spec)


    def getSourceTableSpecs(self):
        return self.srctable_specs_


    def setSourceTableSpecs(self, sts):
        self.srctable_specs_ = sts


    def getAggregationRuleSpec(self):
        return self.aggrule_spec_


    def setAggregationRuleSpec(self, ars):
        self.aggrule_spec_ = ars


    def getImportTableFilterSpec(self):
        return self.filter_spec_


    def setImportTableFilterSpec(self, itf):
        self.filter_spec_ = itf


    def SpecLatticeNamedElements( self ):

        defn =  'SpecLatticeNamedElements(('
        defn +=   'SpecLatticeNamedElement('
        defn +=     'SpecLatticeSourceTable('
        defn +=       'SpecLatticeSourceTableColumnSet(('

        sep = ''
        for c in self.columns_:
            defn += sep
            defn += c.definition()
            sep = ', '

        defn +=       '))'
        defn +=     ', '
        defn += self.getSourceTableSpecs()
        defn +=     ')'
        defn +=   ', ContainerElementName(\"{0}\")'.format( self.getName() )
        defn +=   ')'
        defn += ', SpecLatticeNamedElement('
        defn +=     'SpecLatticeImportTable('
        defn += self.getAggregationRuleSpec()
        defn +=     ', SpecColumnBindings(('

        sep = ''
        for c in self.columns_:
            defn += sep
            defn += 'SpecColumnBinding(ContainerElementName(\"{0}\"),{1})'.format( c.getName(), 'DataType' + c.getDatatype() )
            sep = ', '

        defn +=       '))'
        defn +=     ', {0}'.format( self.getImportTableFilterSpec() )
        defn +=     ')'
        defn +=   ', ContainerElementName(\"{0}\")'.format( self.getName() + '_Import' )
        defn +=   ')'
        defn += ', SpecLatticeNamedElement('
        defn +=     'SpecLatticeBinder('
        defn +=       'SpecBoundName(ContainerElementName(\"{0}\"),NameTypeImportTable)'.format( self.getName() + '_Import' )
        defn +=     ', SpecBoundName(ContainerElementName(\"{0}\"),NameTypeSourceTable)'.format( self.getName() )
        defn +=     ')'
        defn +=   ', ContainerElementName(\"Binder_I2S_T_{0}_{1}\")'.format( self.getName() + '_Import', self.getName() )
        defn +=   ')'
        defn += '))'

        return defn


    def initFromValues(self, name, columns, srctable_specs = None, aggrule_spec = None, filter_spec = None):

        super(TableVDBImpl, self).initFromValues(name, columns)

        if srctable_specs is not None:
            self.srctable_specs_ = srctable_specs

        else:
            self.srctable_specs_ =    'SpecKeys(empty)'
            self.srctable_specs_ += ', SpecDescription("")'
            self.srctable_specs_ += ', SpecMaximalIsMaximal'
            self.srctable_specs_ += ', SpecKeyAggregation(SpecColumnAggregationRuleMostRecent)'
            self.srctable_specs_ += ', SpecEquivalenceAggregation(SpecColumnAggregationRuleMostRecent)'

        if aggrule_spec is not None:
            self.aggrule_spec_ = aggrule_spec

        else:
            self.aggrule_spec_ = 'SpecSourceAggregationRuleMostRecent(SpecTotalOrderEffectiveDate)'

        if filter_spec is not None:
            self.filter_spec_ = filter_spec

        else:
            self.filter_spec_ = 'SpecSourceFilterNone'



    




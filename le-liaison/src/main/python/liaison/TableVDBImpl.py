
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import re
from .exceptions import MaudeStringError
from .Table import Table
from .TableColumnVDBImpl import TableColumnVDBImpl


class TableVDBImpl( Table ):

## This is the default constructor

    def __init__( self, name, columns, srctable_specs = None, aggrule_spec = None, filter_spec = None ):

        self.InitFromValues( name, columns, srctable_specs, aggrule_spec, filter_spec )


## This is the pythonic way to create multiple constructors.  The call is
## t = TableVDBImpl.InitFromDefn( srctable_defn, imptable_defn )

    @classmethod
    def InitFromDefn( cls, name, srctable_defn, imptable_defn ):

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

                c = TableColumnVDBImpl.InitFromDefn( s2.group(1) )
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

        return cls( name, columns, srctable_specs, aggrule_spec, filter_spec )


    def SourceTableSpecs( self ):
        return self._srctable_specs


    def SetSourceTableSpecs( self, sts ):
        self._srctable_specs = sts
        return self._srctable_specs


    def AggregationRuleSpec( self ):
        return self._aggrule_spec


    def SetAggregationRuleSpec( self, ars ):
        self._aggrule_spec = ars
        return self._aggrule_spec


    def ImportTableFilterSpec( self ):
        return self._filter_spec


    def SetImportTableFilterSpec( self, itf ):
        self._filter_spec = itf
        return self._filter_spec


    def SpecLatticeNamedElements( self ):

        defn =  'SpecLatticeNamedElements(('
        defn +=   'SpecLatticeNamedElement('
        defn +=     'SpecLatticeSourceTable('
        defn +=       'SpecLatticeSourceTableColumnSet(('

        sep = ''
        for c in self._columns:
            defn += sep
            defn += c.Definition()
            sep = ', '

        defn +=       '))'
        defn +=     ', '
        defn += self.SourceTableSpecs()
        defn +=     ')'
        defn +=   ', ContainerElementName(\"{0}\")'.format( self.Name() )
        defn +=   ')'
        defn += ', SpecLatticeNamedElement('
        defn +=     'SpecLatticeImportTable('
        defn += self.AggregationRuleSpec()
        defn +=     ', SpecColumnBindings(('

        sep = ''
        for c in self._columns:
            defn += sep
            defn += 'SpecColumnBinding(ContainerElementName(\"{0}\"),{1})'.format( c.Name(), 'DataType' + c.Datatype() )
            sep = ', '

        defn +=       '))'
        defn +=     ', {0}'.format( self.ImportTableFilterSpec() )
        defn +=     ')'
        defn +=   ', ContainerElementName(\"{0}\")'.format( self.Name() + '_Import' )
        defn +=   ')'
        defn += ', SpecLatticeNamedElement('
        defn +=     'SpecLatticeBinder('
        defn +=       'SpecBoundName(ContainerElementName(\"{0}\"),NameTypeImportTable)'.format( self.Name() + '_Import' )
        defn +=     ', SpecBoundName(ContainerElementName(\"{0}\"),NameTypeSourceTable)'.format( self.Name() )
        defn +=     ')'
        defn +=   ', ContainerElementName(\"Binder_I2S_T_{0}_{1}\")'.format( self.Name() + '_Import', self.Name() )
        defn +=   ')'
        defn += '))'

        return defn


    def InitFromValues( self, name, columns, srctable_specs = None, aggrule_spec = None, filter_spec = None ):

        super( TableVDBImpl, self ).InitFromValues( name, columns )

        if srctable_specs is not None:
            self._srctable_specs = srctable_specs

        else:
            self._srctable_specs =    'SpecKeys(empty)'
            self._srctable_specs += ', SpecDescription("")'
            self._srctable_specs += ', SpecMaximalIsMaximal'
            self._srctable_specs += ', SpecKeyAggregation(SpecColumnAggregationRuleMostRecent)'
            self._srctable_specs += ', SpecEquivalenceAggregation(SpecColumnAggregationRuleMostRecent)'

        if aggrule_spec is not None:
            self._aggrule_spec = aggrule_spec

        else:
            self._aggrule_spec = 'SpecSourceAggregationRuleMostRecent(SpecTotalOrderEffectiveDate)'

        if filter_spec is not None:
            self._filter_spec = filter_spec

        else:
            self._filter_spec = 'SpecSourceFilterNone'



    




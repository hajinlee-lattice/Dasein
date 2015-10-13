
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

from .exceptions import UnknownMetadataValue

class QueryColumn( object ):
 
    def __init__( self, name, expression, approved_usage = None, display_name = None,
                                category = None, statistical_type = None, tags = None,
                                fundamental_type = None, description = None,
                                display_discretization = None ):

        self.InitFromValues( name, expression, approved_usage, display_name, category,
                                                 statistical_type, tags, fundamental_type,
                                                 description, display_discretization )


    def Name( self ):
        return self._name

    def SetName( self, n ):
        self._name = n
        return self._name

    def Expression( self ):
        return self._exp

    def SetExpression( self, e ):
        self._exp = e
        return self._exp

    def ApprovedUsage( self ):
        return self._approved_usage

    def SetApprovedUsage( self, au ):
        if au is not None and au not in [ 'None', 'Model', 'ModelAndModelInsights', 'ModelAndAllInsights' ]:
            raise UnknownMetadataValue( au )
        self._approved_usage = au
        return self._approved_usage

    def DisplayName( self ):
        return self._display_name

    def SetDisplayName( self, dn ):
        if dn is not None and len(dn) > 46:
            print 'Warning: truncating DisplayName \'{0}\' to 46 characters'.format( dn )
            self._display_name = dn[:46]
        else:
            self._display_name = dn
        return self._display_name

    def Category( self ):
        return self._category

    def SetCategory( self, c ):
        if c is not None and len(c) > 22:
            print 'Warning: truncating Category \'{0}\' to 22 characters'.format( c )
            self._category = c[:22]
        else:
            self._category = c
        return self._category

    def StatisticalType( self ):
        return self._statistical_type

    def SetStatisticalType( self, st ):
        if st is not None and st not in [ 'nominal', 'ratio', 'ordinal', 'interval' ]:
            raise UnknownMetadataValue( st )
        self._statistical_type = st
        return self._statistical_type

    def Tags( self ):
        return self._tags

    def SetTags( self, t ):
        if t is not None and t.lower() not in [ 'internal', 'external' ]:
            raise UnknownMetadataValue( t )
        self._tags = t
        return self._tags

    def FundamentalType( self ):
        return self._fundamental_type

    def SetFundamentalType( self, ft ):
        if ft is not None and ft not in [ 'numeric', 'currency', 'percentage', 'boolean', 'year' ]:
            raise UnknownMetadataValue( ft )
        self._fundamental_type = ft
        return self._fundamental_type

    def Description( self ):
        return self._description

    def SetDescription( self, d ):
        if d is not None and len(d) > 150:
            print 'Warning: truncating Description \'{0}\' to 150 characters'.format( d )
            self._description = d[:150]
        else:
            self._description = d
        return self._description

    def DisplayDiscretization( self ):
        return self._display_discretization

    def SetDisplayDiscretization( self, dd ):
        self._display_discretization = dd
        return self._display_discretization

    def SetMetadata( self, metadata_type, metadata_value ):
        if metadata_type == 'ApprovedUsage':
            self.SetApprovedUsage( metadata_value )
        elif metadata_type == 'DisplayName':
            self.SetDisplayName( metadata_value )
        elif metadata_type == 'Category':
            self.SetCategory( metadata_value )
        elif metadata_type == 'StatisticalType':
            self.SetStatisticalType( metadata_value )
        elif metadata_type == 'Tags':
            self.SetTags( metadata_value )
        elif metadata_type == 'FundamentalType':
            self.SetFundamentalType( metadata_value )
        elif metadata_type == 'Description':
            self.SetDescription( metadata_value )
        elif metadata_type == 'DisplayDiscretization':
            self.SetDisplayDiscretization( metadata_value )

    def Definition( self ):
        raise NotImplementedError( 'QueryColumn.Definition()' )

    def InitFromValues( self, name, expression, approved_usage = None, display_name = None,
                                            category = None, statistical_type = None, tags = None,
                                            fundamental_type = None, description = None,
                                            display_discretization = None ):

        self.SetName( name )
        self.SetExpression( expression )
        self.SetApprovedUsage( approved_usage )
        self.SetDisplayName( display_name )
        self.SetCategory( category )
        self.SetStatisticalType( statistical_type )
        self.SetTags( tags )
        self.SetFundamentalType( fundamental_type )
        self.SetDescription( description )
        self.SetDisplayDiscretization( display_discretization )

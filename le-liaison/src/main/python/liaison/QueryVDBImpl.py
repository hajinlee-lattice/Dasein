
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

import re
from .exceptions import MaudeStringError, ExpressionNotImplemented
from .Query import Query
from .QueryColumnVDBImpl import QueryColumnVDBImpl
from .QueryEntityVDBImpl import QueryEntityVDBImpl
from .QueryFilterVDBImpl import QueryFilterVDBImpl


class QueryVDBImpl( Query ):

    ## This is the default constructor

    def __init__( self, name, columns, filters = None, entities = None, sqrs_spec = None ):

        self.InitFromValues( name, columns, filters, entities, sqrs_spec )
    

## This is the pythonic way to create multiple constructors.  The call is
## q = QueryVDBImpl.InitFromDefn( defn )

    @classmethod
    def InitFromDefn( cls, name, defn ):

        s1 = re.search( '^SpecLatticeQuery\((LatticeAddressSet.*), SpecQueryNamedFunctions\((.*)\), (SpecQueryResultSet.*)\)$', defn )
        if not s1:
            raise MaudeStringError( defn )

        las_spec  = s1.group(1)

        lasm = ''
        lae = ''
        isMatchedTopLevel = False

        s3 = re.search( '^LatticeAddressSetPushforward\((LatticeAddressExpression.*), LatticeAddressSetMeet\((.*?)\), (LatticeAddressExpressionAtomic\(.*?\))\)$', las_spec )
        if s3:
            lasm = s3.group(2)
            lae = '('+s3.group(3)+')'
            isMatchedTopLevel = True

        if not isMatchedTopLevel:
            s3 = re.search( '^LatticeAddressSetPushforward\((LatticeAddressExpression.*), LatticeAddressSetMeet\((.*?)\), LatticeAddressExpressionMeet\((.*?)\)\)$', las_spec )
            if not s3:
                raise MaudeStringError( las_spec )

            lasm = s3.group(2)
            lae = s3.group(3)
            isMatchedTopLevel = True

        filters = []
        entities = []
        
        if lasm != 'empty':

            lasm = lasm[1:-1]

            while True:
                
                filterspec = ''
                remainingspec = ''
                isMatched = False

                s31 = re.search( '^(LatticeAddressSetFcn\(LatticeFunction.*?LatticeAddressSet.*?\))(, LatticeAddressSet.*|$)', lasm )
                if s31:
                    filterspec = s31.group(1)
                    remainingspec = s31.group(2)
                    isMatched = True

                if not isMatched:
                    s31 = re.search( '^(LatticeAddressSet(.*?)\((.*?)\))(, LatticeAddressSet.*|$)', lasm )
                    if not s31:
                        raise MaudeStringError( lasm )

                    if s31.group(2) in set(['Fcn','AndNot','Meet','Join','Pushforward','Pullback','Relation','Copy','Port','Level','Conflict','PivotFunctions']):
                        raise ExpressionNotImplemented( 'LatticeAddressSet{0}'.format(s31.group(2)) )

                    filterspec = s31.group(1)
                    remainingspec = s31.group(4)
                    isMatched = True

                filters.append( QueryFilterVDBImpl(filterspec) )
                if remainingspec == '':
                    break
                else:
                    # Strip off the ', ' at the beginning
                    lasm = remainingspec[2:]
        
        if lae != 'empty':

            lae = lae[1:-1]

            while True:

                entityspec = ''
                remainingspec = ''
                isMatched = False

                if not isMatched:

                    s32 = re.search( '^(LatticeAddressExpression(.*?)\((.*?)\))(, LatticeAddressExpression.*|$)', lae )
                    if not s32:
                        raise MaudeStringError( lae )

                    if s32.group(2) in set(['Join','Meet','Copy','Relation','DivideIntoExistingNode','DivideNewNode','Port']):
                        raise ExpressionNotImplemented( 'LatticeAddressExpression{0}'.format(s32.group(2)) )

                    entityspec = s32.group(1)
                    remainingspec = s32.group(4)
                    isMatched = True

                entities.append( QueryEntityVDBImpl(entityspec) )
                if remainingspec == '':
                    break
                else:
                    # Strip off the ', ' at the beginning
                    lae = remainingspec[2:]

        sqrs_spec = s1.group(3)

        columns = []
        sqnf = s1.group(2)

        ## Iterate on the comma-separated SpecLatticeSourceTableColumn specs
        while True:
            s2 = re.search( '^(SpecQueryNamedFunction.*?)( SpecQueryNamedFunction.*|$)', sqnf )
                
            if not s2:
                raise MaudeStringError( sqnf )

            c = QueryColumnVDBImpl.InitFromDefn( s2.group(1) )
            columns.append( c )

            if s2.group(2) == '':
                break
            else:
                # Strip off the ' ' at the beginning
                sqnf = s2.group(2)[1:]

        return cls( name, columns, filters, entities, sqrs_spec )


    def InitFromValues( self, name, columns, filters = None, entities = None, sqrs_spec = None ):

        super( QueryVDBImpl, self ).InitFromValues( name, columns )

        self._filters = []

        if filters is not None:
            self._filters = filters

        self._entities = []

        if entities is not None:
            self._entities = entities

        #if las_spec is not None:
        #  self._las_spec = las_spec

        #else:
        #  self._las_spec =  'LatticeAddressSetPushforward('
        #  self._las_spec +=   'LatticeAddressExpressionFromLAS(LatticeAddressSetMeet(empty))'
        #  self._las_spec += ', LatticeAddressSetMeet(empty)'
        #  self._las_spec += ', LatticeAddressExpressionMeet(empty)'
        #  self._las_spec += ')'

        if sqrs_spec is not None:
            self._sqrs_spec = sqrs_spec

        else:
            self._sqrs_spec = 'SpecQueryResultSetAll'


    def SpecLatticeNamedElements( self ):

        defn =  'SpecLatticeNamedElements(('
        defn +=   'SpecLatticeNamedElement('
        defn +=     'SpecLatticeQuery('
        defn +=       'LatticeAddressSetPushforward('

        if not self._filters:
            defn +=       'LatticeAddressExpressionFromLAS(LatticeAddressSetMeet(empty))'
            defn +=     ', LatticeAddressSetMeet(empty)'
        else:
            sep = ''
            tmplas =      'LatticeAddressSetMeet(('
            for f in self._filters:
                tmplas += sep
                tmplas += f.Definition()
                sep = ', '
            tmplas +=     '))'
            defn +=       'LatticeAddressExpressionFromLAS({0})'.format( tmplas )
            defn +=     ', {0}'.format( tmplas )

        if not self._entities:
            defn +=     ', LatticeAddressExpressionMeet(empty)'
        else:
            sep = ''
            defn +=     ', LatticeAddressExpressionMeet(('
            for e in self._entities:
                defn += sep
                defn += e.Definition()
                sep = ', '
            defn +=       '))'

        defn +=       ')'
        defn +=     ', SpecQueryNamedFunctions('

        sep = ''
        for c in self._columns:
            defn += sep
            defn += c.Definition()
            sep = ' '

        defn +=       ')'
        defn +=     ', ' + self._sqrs_spec
        defn +=     ')'
        defn +=   ', ContainerElementName(\"{0}\")'.format( self.Name() )
        defn +=   ')'
        defn += '))'

        return defn

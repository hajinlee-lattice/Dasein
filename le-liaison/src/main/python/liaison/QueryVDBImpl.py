
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


class QueryVDBImpl(Query):

    ## This is the default constructor

    def __init__(self, name, columns, filters = [], entities = [], resultSet = None):

        self.initFromValues(name, columns, filters, entities, resultSet)
    

## This is the pythonic way to create multiple constructors.  The call is
## q = QueryVDBImpl.InitFromDefn( defn )

    @classmethod
    def initFromDefn(cls, name, defn):

        s1 = re.search( '^SpecLatticeQuery\((LatticeAddressSet.*), SpecQueryNamedFunctions\((.*)\), (SpecQueryResultSet.*)\)$', defn )
        if not s1:
            raise MaudeStringError( defn )

        las_spec  = s1.group(1)

        lasm = ''
        lae = ''
        isMatchedTopLevel = False

        if not isMatchedTopLevel:
            s3 = re.search( '^LatticeAddressSetPushforward\(LatticeAddressExpressionFromLAS\(LatticeAddressSetMeet\((.*?)\)\), LatticeAddressSetMeet\((.*?)\), LatticeAddressExpressionMeet\((.*)\)\)$', las_spec )
            if s3:
                lasm = s3.group(2)
                lae = s3.group(3)
                isMatchedTopLevel = True

        if not isMatchedTopLevel:
            s3 = re.search( '^LatticeAddressSetPushforward\(LatticeAddressExpressionFromLAS\(LatticeAddressSetMeet\((.*?)\)\), LatticeAddressSetMeet\((.*?)\), (LatticeAddressExpressionAtomic\(.*\))\)$', las_spec )
            if s3:
                lasm = s3.group(2)
                lae = '({0})'.format( s3.group(3) )
                isMatchedTopLevel = True

        if not isMatchedTopLevel:
            s3 = re.search( '^LatticeAddressSetPushforward\(LatticeAddressExpressionFromLAS\((LatticeAddressSetIdentifier\(.*?\))\), (LatticeAddressSetIdentifier\(.*?\)), LatticeAddressExpressionMeet\((.*)\)\)$', las_spec )
            if s3:
                lasm = '({0})'.format( s3.group(2) )
                lae = s3.group(3)
                isMatchedTopLevel = True

        if not isMatchedTopLevel:
            s3 = re.search( '^LatticeAddressSetPushforward\(LatticeAddressExpressionFromLAS\((LatticeAddressSetIdentifier\(.*?\))\), (LatticeAddressSetIdentifier\(.*?\)), (LatticeAddressExpressionAtomic\(.*\))\)$', las_spec )
            if s3:
                lasm = '({0})'.format( s3.group(2) )
                lae = '({0})'.format( s3.group(3) )
                isMatchedTopLevel = True

        if not isMatchedTopLevel:
            s3 = re.search( '^LatticeAddressSetPushforward\(LatticeAddressExpressionMeet\((.*?)\), LatticeAddressSetMeet\((.*?)\), LatticeAddressExpressionMeet\((.*)\)\)$', las_spec )
            if s3:
                lasm = s3.group(2)
                lae = s3.group(3)
                isMatchedTopLevel = True

        if not isMatchedTopLevel:
            s3 = re.search( '^LatticeAddressSetPushforward\(LatticeAddressExpressionMeet\((.*?)\), LatticeAddressSetMeet\((.*?)\), (LatticeAddressExpressionAtomic\(.*\))\)$', las_spec )
            if s3:
                lasm = s3.group(2)
                lae = '({0})'.format( s3.group(3) )
                isMatchedTopLevel = True

        if not isMatchedTopLevel:
            raise MaudeStringError( las_spec )

        
        filters = []
        entities = []
        
        if lasm != 'empty':

            lasm = lasm[1:-1]

            while True:
                
                filterspec = ''
                remainingspec = ''
                isMatched = False

                if not isMatched:
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
                        raise ExpressionNotImplemented( 'LatticeAddressSet{0}: {1}'.format(s31.group(2),lasm) )

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

        resultSet = s1.group(3)

        columns = []
        sqnf = s1.group(2)

        ## Iterate on the comma-separated SpecLatticeSourceTableColumn specs
        while True:
            s2 = re.search( '^(SpecQueryNamedFunction.*?)( SpecQueryNamedFunction.*|$)', sqnf )
                
            if not s2:
                raise MaudeStringError( sqnf )

            c = QueryColumnVDBImpl.initFromDefn( s2.group(1) )
            columns.append( c )

            if s2.group(2) == '':
                break
            else:
                # Strip off the ' ' at the beginning
                sqnf = s2.group(2)[1:]

        return cls(name, columns, filters, entities, resultSet)


    def initFromValues(self, name, columns, filters, entities, resultSet = None):

        super(QueryVDBImpl, self).initFromValues(name, columns, filters, entities, resultSet)

        if resultSet is not None:
            self.resultSet_ = resultSet

        else:
            self.resultSet_ = 'SpecQueryResultSetAll'


    def SpecLatticeNamedElements(self):

        defn =  'SpecLatticeNamedElements(('
        defn +=   'SpecLatticeNamedElement('
        defn +=     'SpecLatticeQuery('
        defn +=       'LatticeAddressSetPushforward('

        if not self.filters_:
            defn +=       'LatticeAddressExpressionFromLAS(LatticeAddressSetMeet(empty))'
            defn +=     ', LatticeAddressSetMeet(empty)'
        else:
            sep = ''
            tmplas =      'LatticeAddressSetMeet(('
            for f in self.filters_:
                tmplas += sep
                tmplas += f.definition()
                sep = ', '
            tmplas +=     '))'
            defn +=       'LatticeAddressExpressionFromLAS({0})'.format( tmplas )
            defn +=     ', {0}'.format( tmplas )

        if not self.entities_:
            defn +=     ', LatticeAddressExpressionMeet(empty)'
        else:
            sep = ''
            defn +=     ', LatticeAddressExpressionMeet(('
            for e in self.entities_:
                defn += sep
                defn += e.definition()
                sep = ', '
            defn +=       '))'

        defn +=       ')'
        defn +=     ', SpecQueryNamedFunctions('

        sep = ''
        for c in self.columns_:
            defn += sep
            defn += c.definition()
            sep = ' '

        defn +=       ')'
        defn +=     ', ' + self.resultSet_
        defn +=     ')'
        defn +=   ', ContainerElementName(\"{0}\")'.format( self.getName() )
        defn +=   ')'
        defn += '))'

        return defn

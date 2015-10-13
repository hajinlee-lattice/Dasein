
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

class NamedExpression( object ):
 
    def __init__( self, name ):

        self.InitFromValues( name )


    def Name( self ):
        return self._name

    def SetName( self, n ):
        self._name = n
        return self._name

    def InitFromValues( self, name ):

        self._name = name

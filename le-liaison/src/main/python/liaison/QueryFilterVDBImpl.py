
#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

class QueryFilterVDBImpl( object ):

    def __init__( self, spec ):

        self.InitFromValues( spec )


    def Definition( self ):

        return self._spec
        

    def InitFromValues( self, spec ):

        self._spec = spec


#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

class QueryEntityVDBImpl( object ):

    def __init__( self, spec ):

        self.InitFromValues( spec )


    def definition( self ):

        return self._spec


    def InitFromValues( self, spec ):

        self._spec = spec


#
# $LastChangedBy$
# $LastChangedDate$
# $Rev$
#

class LoadGroupMgr( object ):

    def __init__( self, conn_mgr ):
        
        self._conn_mgr = conn_mgr


    def createLoadGroup( self, groupName, groupPath, groupAlias, autoClear, isNestedGroup ):
        raise NotImplementedError( 'LoadGroupMgr.createLoadGroup()' )


    def deleteLoadGroup( self, groupName ):
        raise NotImplementedError( 'LoadGroupMgr.deleteLoadGroup()' )


    def getLoadGroup( self, groupName ):
        raise NotImplementedError( 'LoadGroupMgr.getLoadGroup()' )


    def setLoadGroup( self, config ):
        raise NotImplementedError( 'LoadGroupMgr.setLoadGroup()' )


    def hasLoadGroup( self, groupName ):
        raise NotImplementedError( 'LoadGroupMgr.hasLoadGroup()' )


    def getLoadGroupFunctionality( self, groupName, functionality ):
        raise NotImplementedError( 'LoadGroupMgr.getLoadGroupFunctionality()' )


    def setLoadGroupFunctionality( self, groupName, config ):
        raise NotImplementedError( 'LoadGroupMgr.setLoadGroupFunctionality()' )


    def commit( self ):
        raise NotImplementedError( 'LoadGroupMgr.commit()' )

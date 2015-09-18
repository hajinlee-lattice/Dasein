package com.latticeengines.liaison.exposed.service;

import com.latticeengines.liaison.exposed.service.ConnectionMgr;
import com.latticeengines.liaison.service.impl.ConnectionMgrVDBImpl;

public class ConnectionMgrFactory {
	
	public static ConnectionMgr getConnectionMgr( String type, String ... args ) throws RuntimeException {
		// Default is to return a visiDB Manager 
		if( args.length != 2 ) {
			throw new RuntimeException( "Invalid number of arguments for ConnectionMgrVDBImpl" );
		}
		return new ConnectionMgrVDBImpl( args[0], args[1] );
	}
}

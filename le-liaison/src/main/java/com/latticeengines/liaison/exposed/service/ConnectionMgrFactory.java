package com.latticeengines.liaison.exposed.service;

public interface ConnectionMgrFactory {
	
	ConnectionMgr getConnectionMgr( String type, String ... args );
}

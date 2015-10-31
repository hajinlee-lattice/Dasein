package com.latticeengines.liaison.exposed.service;

import com.latticeengines.liaison.exposed.service.ConnectionMgr;

public interface ConnectionMgrFactory {
	
	ConnectionMgr getConnectionMgr( String type, String ... args );
}

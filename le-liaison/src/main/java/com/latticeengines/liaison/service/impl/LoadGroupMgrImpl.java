package com.latticeengines.liaison.service.impl;

import java.io.IOException;

import com.latticeengines.liaison.exposed.exception.UnknownDataLoaderObjectException;
import com.latticeengines.liaison.exposed.service.ConnectionMgr;
import com.latticeengines.liaison.exposed.service.LoadGroupMgr;

public class LoadGroupMgrImpl implements LoadGroupMgr {

    private final ConnectionMgr conn_mgr;

    public LoadGroupMgrImpl(ConnectionMgr conn_mgr, String configfile) {
        this.conn_mgr = conn_mgr;
    }

    public Boolean hasLoadGroup(String groupName) {
        return Boolean.FALSE;
    }

    public String getLoadGroupFunctionality(String groupName, String functionality)
            throws UnknownDataLoaderObjectException {
        return "Not implemented";
    }

    public void setLoadGroupFunctionality(String groupName, String xmlConfig)
            throws UnknownDataLoaderObjectException {
        
    }

    public void commit() throws IOException, RuntimeException {
        this.conn_mgr.installDLConfigFile(getConfig());
    }

    public String getConfig() {
        return "Not implemented";
    }
}

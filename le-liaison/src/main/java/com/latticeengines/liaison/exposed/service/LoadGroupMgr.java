package com.latticeengines.liaison.exposed.service;

import java.io.IOException;

import com.latticeengines.liaison.exposed.exception.UnknownDataLoaderObjectException;

public interface LoadGroupMgr {

    // hasLoadGroup(...)
    //
    // Returns a boolean indicating whether or not the tenant has the load group named

    Boolean hasLoadGroup(String groupName);

    // getLoadGroupFunctionality(...)
    //
    // Returns an xml representation of the requested load group functionality.

    String getLoadGroupFunctionality(String groupName, String functionality)
            throws UnknownDataLoaderObjectException;

    // setLoadGroupFunctionality(...)
    //
    // Sets load group functionality from an xml representation

    void setLoadGroupFunctionality(String groupName, String xmlConfig)
            throws UnknownDataLoaderObjectException;

    // commit()
    //
    // This method commits all accumulated updates to the DataLoader tenant.

    void commit() throws IOException, RuntimeException;

}

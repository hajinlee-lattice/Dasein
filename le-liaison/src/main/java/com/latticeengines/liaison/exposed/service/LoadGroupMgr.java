package com.latticeengines.liaison.exposed.service;

import java.io.IOException;

import javax.xml.transform.TransformerException;

import org.w3c.dom.Element;

import com.latticeengines.liaison.exposed.exception.UnknownDataLoaderObjectException;

public interface LoadGroupMgr {

    // hasLoadGroup(...)
    //
    // Returns a boolean indicating whether or not the tenant has the load group named

    Boolean hasLoadGroup(String groupName);

    // getLoadGroupFunctionalityXML(...)
    //
    // Returns an xml representation of the requested load group functionality.

    String getLoadGroupFunctionalityXML(String groupName, String functionality)
            throws UnknownDataLoaderObjectException, TransformerException;

    // getLoadGroupFunctionality(...)
    //
    // Returns an DOM Element representation of the requested load group functionality.

    Element getLoadGroupFunctionality(String groupName, String functionality)
            throws UnknownDataLoaderObjectException;

    // setLoadGroupFunctionalityXML(...)
    //
    // Sets load group functionality from an xml representation

    void setLoadGroupFunctionalityXML(String groupName, String xmlConfig)
            throws UnknownDataLoaderObjectException, RuntimeException;

    // setLoadGroupFunctionality(...)
    //
    // Sets load group functionality from a DOM Element representation

    void setLoadGroupFunctionality(String groupName, Element elem)
            throws UnknownDataLoaderObjectException;

    // commit()
    //
    // This method commits all accumulated updates to the DataLoader tenant.

    void commit() throws IOException, RuntimeException;

}

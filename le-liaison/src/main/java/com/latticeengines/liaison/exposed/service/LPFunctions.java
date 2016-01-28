package com.latticeengines.liaison.exposed.service;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

public interface LPFunctions {

    // getLPTemplateTypeAndVersion(...)
    //
    // Returns a pair (type,version) for standard LP DL/visiDB templates. If the
    // tenant is not on a standard
    // template, then the type and version are set to "No template type/version"
    // (there is no version information)
    // or "Nonstandard type/verion" (the version information cannot be parsed".
    // If there is a connection problem, an IOException is thrown.
    // If the DL REST API connection does not return success, then a
    // RuntimeException is thrown.

    AbstractMap.SimpleImmutableEntry<String, String> getLPTemplateTypeAndVersion(
            ConnectionMgr conn_mgr);

    // addLDCMatch(...)
    //
    // Adds a new match to the Lattice Data Cloud (LDC, or "PropData") to a
    // standard DL/visiDB template. The argument "source"
    // species the LDC source (e.g., "DerivedColumns", "HGData_Pivoted_Source").
    // A boolean is returned indicating success.
    // If there is a connection problem, an IOException is thrown.
    // If the DL REST API connection does not return success, then a
    // RuntimeException is thrown.

    Boolean addLDCMatch(ConnectionMgr conn_mgr, String source, String lp_template_version)
            throws IOException, RuntimeException;

    // setLDCWritebackAttributes(...)
    //
    // Sets Lattice Data Cloud (LDC, or "PropData") attributes to be written
    // back to the customer in a standard DL/visiDB template.
    // The argument "source" species the LDC source (e.g., "DerivedColumns",
    // "HGData_Pivoted_Source").
    // The attributes map specifies
    // (column_name_in_propdata,column_name_in_customer_system).
    // A boolean is returned indicating success.
    // If there is a connection problem, an IOException is thrown.
    // If the DL REST API connection does not return success, then a
    // RuntimeException is thrown.

    Boolean setLDCWritebackAttributes(ConnectionMgr conn_mgr, String source,
            Map<String, String> attributes, String lp_template_version)
            throws IOException, RuntimeException;

    // getLDCWritebackAttributes(...)
    //
    // Gets Lattice Data Cloud (LDC, or "PropData") attributes to be written
    // back to the customer in a standard DL/visiDB template.  The attributes
    // are returned in a map (column_name_in_propdata,column_name_in_customer_system).
    // If there is a connection problem, an IOException is thrown.
    // If the DL REST API connection does not return success, then a
    // RuntimeException is thrown.

    Map<String, String> getLDCWritebackAttributes(ConnectionMgr conn_mgr, String source,
            String lp_template_version)
            throws IOException, RuntimeException;

    // setLDCWritebackAttributesDefaultName(...)
    //
    // Similar to the above method, but the "column_name_in_customer_system" is derived
    // from "column_name_in_propdata" according to the lp_template_type.

    Boolean setLDCWritebackAttributesDefaultName(ConnectionMgr conn_mgr, String source,
            Set<String> column_names_in_propdata, String lp_template_type,
            String lp_template_version) throws IOException, RuntimeException;

    // removeLDCWritebackAttributes(...)
    //
    // Removes all the attributes to be written back to teh customer.

    void removeLDCWritebackAttributes(ConnectionMgr conn_mgr, String lp_template_version)
            throws IOException, RuntimeException;
}

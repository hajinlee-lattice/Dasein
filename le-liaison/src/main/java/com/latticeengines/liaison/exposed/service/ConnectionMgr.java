package com.latticeengines.liaison.exposed.service;

import java.io.IOException;
import java.util.Map;

public interface ConnectionMgr {

    Query getQuery(String queryName) throws IOException, RuntimeException;

    Map<String, Map<String, String>> getMetadata(String queryName) throws IOException,
            RuntimeException;

    void setQuery(Query query) throws IOException, RuntimeException;

    LoadGroupMgr getLoadGroupMgr() throws IOException, RuntimeException;

    void installDLConfigFile(String config) throws IOException, RuntimeException;
}

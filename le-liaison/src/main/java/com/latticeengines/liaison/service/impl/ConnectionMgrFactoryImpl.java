package com.latticeengines.liaison.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.liaison.exposed.service.ConnectionMgr;
import com.latticeengines.liaison.exposed.service.ConnectionMgrFactory;
import com.latticeengines.remote.exposed.service.DataLoaderService;

@Component
public class ConnectionMgrFactoryImpl implements ConnectionMgrFactory {

    @Autowired
    private DataLoaderService dataLoaderService;

    @Override
    public ConnectionMgr getConnectionMgr(String type, String... args) throws RuntimeException {
        // Default is to return a visiDB Manager
        if (args.length != 2) {
            throw new RuntimeException("Invalid number of arguments for ConnectionMgrVDBImpl");
        }
        return new ConnectionMgrVDBImpl(args[0], args[1], dataLoaderService);
    }
}

package com.latticeengines.liaison.service.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.liaison.exposed.service.ConnectionMgr;
import com.latticeengines.liaison.exposed.service.ConnectionMgrFactory;
import com.latticeengines.liaison.exposed.service.Query;
import com.latticeengines.liaison.exposed.service.QueryColumn;
import com.latticeengines.liaison.testframework.LiaisonTestNGBase;

public class ConnectionMgrFactoryImplTestNG extends LiaisonTestNGBase {

    @Autowired
    private ConnectionMgrFactory connectionMgrFactory;

    @Test(groups = "functional")
    public void testConnection() {
        String tenantName = "ProductionTestPLSTenant2";
        String dlURL = "https://data-pls.lattice-engines.com/Dataloader_PLS/";

        String queryName = "Q_PLS_Modeling";
        
        ConnectionMgr conn_mgr = connectionMgrFactory.getConnectionMgr( "visiDB", tenantName, dlURL );
        
        try {
            
            Map< String, Map<String,String>> modelcols = conn_mgr.getMetadata( queryName );
            
            // This is the sorted set of all the column names
            TreeSet<String> colNames = new TreeSet<>( modelcols.keySet() );

            // Get the query
            Query q = conn_mgr.getQuery( queryName );

            // Get a column you wish to update
            QueryColumn qc = q.getColumn("Title_IsTechRelated");

            // Put the updated metadata in a map
            Map<String,String> newMetadata = new HashMap<>();
            newMetadata.put("Category","Lead Information");
            newMetadata.put("StatisticalType","nominal");

            // Update the column
            qc.setMetadata( newMetadata );

            // Update the query with the updated column
            q.updateColumn( qc );

            // Write back to visiDB
            conn_mgr.setQuery( q );
        }
        catch (IOException ex) {
            System.out.println( String.format("Exception: %s",ex.getMessage()) );
        }

    }

}

package com.latticeengines.liaison.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.liaison.exposed.service.ConnectionMgr;
import com.latticeengines.liaison.exposed.service.ConnectionMgrFactory;
import com.latticeengines.liaison.exposed.service.Query;
import com.latticeengines.liaison.exposed.service.QueryColumn;
import com.latticeengines.liaison.testframework.LiaisonTestNGBase;

public class SynchLPModelingAndScoringTestNG extends LiaisonTestNGBase {
	
	private static final String modelingQueryName = "Q_PLS_Modeling";
	private static final String scoringBulkQueryName = "Q_PLS_Scoring_Bulk";
	private static final String scoringIncrQueryName = "Q_PLS_Scoring_Incremental";
	
	private static final String eventFcnName = "P1_Event";
	private static final String modelGUIDFcnName = "Model_GUID";

    @Autowired
    private ConnectionMgrFactory connectionMgrFactory;

    @Test(groups = "functional")
    public void synchLPModelingAndScoring() {
    	
        String tenantName = "ProductionTestPLSTenant2";
        String dlURL = "https://data-pls.lattice-engines.com/Dataloader_PLS/";
        
        ConnectionMgr conn_mgr = connectionMgrFactory.getConnectionMgr( "visiDB", tenantName, dlURL );

        try {
        	
        	Query modelingQuery = conn_mgr.getQuery(modelingQueryName);
        	Query scoringIncrQuery = conn_mgr.getQuery(scoringIncrQueryName);
        	
        	List<String> colnames_scoring_orig = scoringIncrQuery.getColumnNames();
        	
        	QueryColumn col_model_guid = scoringIncrQuery.getColumn(modelGUIDFcnName);
        	QueryColumn col_event_null = scoringIncrQuery.getColumn(eventFcnName);
        	
        	List<QueryColumn> cols_scoring = new ArrayList<>();
        	List<QueryColumn> cols_modeling = modelingQuery.getColumns();
        	
        	for(QueryColumn c : cols_modeling) {
        		if(!c.getName().equals("P1_Event")) {
        			cols_scoring.add(c);
        		}
        		else {
        			cols_scoring.add(col_model_guid);
        			cols_scoring.add(col_event_null);
        		}
        	}
        	
        	Boolean updateRequired = Boolean.FALSE;
        	
        	for(QueryColumn c : cols_scoring) {
        		if(!colnames_scoring_orig.contains(c.getName())) {
        			updateRequired = Boolean.TRUE;
        			break;
        		}
        	}
        	
        	if(updateRequired) {
        	    System.out.println("Update required");
        		Query scoringBulkQuery = conn_mgr.getQuery(scoringBulkQueryName);
        		scoringBulkQuery.setColumns(cols_scoring);
        		scoringIncrQuery.setColumns(cols_scoring);
        		conn_mgr.setQuery(scoringBulkQuery);
                conn_mgr.setQuery(scoringIncrQuery);
        	}
        }
        catch (IOException ex) {
            System.out.println( String.format("Exception: %s",ex.getMessage()) );
        }
        
    }

}

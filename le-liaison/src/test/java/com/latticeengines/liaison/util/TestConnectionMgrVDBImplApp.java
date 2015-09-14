package com.latticeengines.liaison.util;


import com.latticeengines.liaison.service.impl.ConnectionMgrVDBImpl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import com.latticeengines.liaison.exposed.service.Query;
import com.latticeengines.liaison.service.impl.QueryVDBImpl;
import com.latticeengines.liaison.exposed.service.QueryColumn;

public class TestConnectionMgrVDBImplApp {

	public static void main(String[] args) {
		
		String tenantName = "MW_Devel_Modeling_ELQ_20150529";
		String dlURL = "https://10.41.1.187:8080/";
		
		String queryName = "Q_PLS_Modeling";
		//String queryName = "Q_Timestamp_PreScoringIncr";
		
		ConnectionMgrVDBImpl conn_mgr = new ConnectionMgrVDBImpl( tenantName, dlURL );
		
		try {
			System.out.print( "Initializing..." );
			Map< String, Map<String,String> > modelcols = conn_mgr.getMetadata( queryName );
			System.out.println( "Done" );
			
			// This is the sorted set of all the column names
			TreeSet<String> colNames = new TreeSet<>( modelcols.keySet() );
			
			// Get the query
			Query q = conn_mgr.getQuery( queryName );
			
			// Get a column you wish to update
			QueryColumn qc = q.getColumn("AlexaRank");
			
			// Put the updated metadata in a map
			Map<String,String> newMetadata = new HashMap<>();
			newMetadata.put("Category","Something New");
			
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
		
		System.out.println( "Finished successfully" );
	}

}

package com.latticeengines.liaison.exposed.service;

import com.latticeengines.liaison.exposed.service.Query;

import java.io.IOException;
import java.util.Map;

public interface ConnectionMgr {

	public Query getQuery( String queryName ) throws IOException, RuntimeException;
	
	public Map< String, Map<String,String> > getMetadata( String queryName ) throws IOException, RuntimeException;
	
	public void setQuery( Query query ) throws IOException, RuntimeException;
	
}

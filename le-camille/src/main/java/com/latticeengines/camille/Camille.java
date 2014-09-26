package com.latticeengines.camille;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class Camille {
	private static final Logger log = LoggerFactory.getLogger(new Object(){}.getClass().getEnclosingClass());
	
	private CuratorFramework client;
	
	// package visibility is deliberate
	Camille(CuratorFramework client) {
		this.client = client;
	}
	
	public CuratorFramework getCuratorClient() {
		return client;
	}
	
	public void create(Path path, Document doc, ACL acl) {
		
	}
	
	public void set(Path path, Document doc) {
		set(path, doc, false);
	}
	
	public void set(Path path, Document doc, boolean force) {
		
	}
	
	public void get(Path path) {
		
	}
	
	public Document get(Path path, CuratorWatcher watcher) {
		return null;
	}
	
	public List<Pair<Document, Path>> getChildren() {
		return null;
	}
	
	public Object getHierarchy(Path path) {
		return null; // TODO: return DocumentHierarchy instead of Object
	}
	
	public void delete(Path path) {
		
	}
	
	public boolean exists(Path path) {
		return false;
	}
}

package com.latticeengines.camille;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.data.ACL;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;

public class Camille {
	public static void create(Path path, Document doc, ACL acl) {
		
	}
	
	public static void set(Path path, Document doc) {
		set(path, doc, false);
	}
	
	public static void set(Path path, Document doc, boolean force) {
		
	}
	
	public static void get(Path path) {
		
	}
	
	public static Document get(Path path, CuratorWatcher watcher) {
		return null;
	}
	
	public static List<Pair<Document, Path>> getChildren() {
		return null;
	}
	
	public static Object getHierarchy(Path path) {
		return null; // TODO: return DocumentHierarchy instead of Object
	}
	
	public static void delete(Path path) {
		
	}
	
	public static boolean exists(Path path) {
		return false;
	}
}

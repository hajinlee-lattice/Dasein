package com.latticeengines.camille;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.api.CuratorWatcher;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentHierarchy;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class ConfigurationController<T extends ConfigurationScope> {
    private T scope;
    
    public ConfigurationController(T scope) {
        this.scope = scope;
    }
    
    public void create(Path path, Document document) {
        // TODO
    }

    public void set(Path path, Document document) {
        set(path, document, false);
    }
    
    public void set(Path path, Document document, boolean force) {
        // TODO
    }
    
    public Document get(Path path) {
        // TODO
        return null;
    }
    
    public Document get(Path path, CuratorWatcher watcher) {
        // TODO
        return null;
    }
    
    public List<Pair<Document,Path>> getChildren(Path path) {
        // TODO
        return null;
    }
    
    public DocumentHierarchy getHierarchy(Path path) {
        // TODO
        return null;
    }
    
    public void delete(Path path) {
        // TODO
    }
    
    public boolean exists(Path path) {
        // TODO
        return false;
    }
}

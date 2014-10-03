package com.latticeengines.camille;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class ConfigurationTransaction<T extends ConfigurationScope> {
    private T scope;
    
    public ConfigurationTransaction(T scope) {
        this.scope = scope;
    }
    
    public void check(Path path, Document document) {
        // TODO
    }
    
    public void create(Path path, Document document) {
        // TODO
    }
    
    public void set(Path path, Document document) {
        // TODO
    }
    
    public void delete(Path path) {
        // TODO
    }
    
    public void commit() {
        // TODO
    }
}

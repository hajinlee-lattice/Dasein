package com.latticeengines.camille.config;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public interface ConfigurationTransactionInterface<T extends ConfigurationScope> {
    
    public void check(Path path, Document document) throws Exception;    
    public void create(Path path, Document document) throws Exception;
    public void set(Path path, Document document) throws Exception;
    public void delete(Path path) throws Exception;
    public void commit() throws Exception;
}

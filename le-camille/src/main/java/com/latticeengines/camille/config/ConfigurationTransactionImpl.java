package com.latticeengines.camille.config;

import com.latticeengines.camille.DocumentSerializationException;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public interface ConfigurationTransactionImpl<T extends ConfigurationScope> {
    
    public void check(Path path, Document document);    
    public void create(Path path, Document document) throws DocumentSerializationException;
    public void set(Path path, Document document) throws DocumentSerializationException;
    public void delete(Path path);
    public void commit() throws Exception;
}

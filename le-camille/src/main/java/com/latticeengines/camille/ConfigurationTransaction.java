package com.latticeengines.camille;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class ConfigurationTransaction<T extends ConfigurationScope> implements ConfigurationTransactionImpl<T> {
    private ConfigurationTransactionImpl<T> impl;
    
    public ConfigurationTransaction(T scope) {
        impl = ConfigurationTransactionImplFactory.getImplementation(scope);
    }
    
    @Override
    public void check(Path path, Document document) {
        impl.check(path,  document);
    }

    @Override
    public void create(Path path, Document document) throws DocumentSerializationException {
        impl.create(path,  document);
    }

    @Override
    public void set(Path path, Document document) throws DocumentSerializationException {
        impl.set(path, document);
    }

    @Override
    public void delete(Path path) {
        impl.delete(path);
    }

    @Override
    public void commit() throws Exception {
        impl.commit();
    }
    
}

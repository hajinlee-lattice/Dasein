package com.latticeengines.camille.exposed.config;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class ConfigurationTransaction<T extends ConfigurationScope> implements ConfigurationTransactionInterface<T> {
    private ConfigurationTransactionInterface<T> impl;

    @SuppressWarnings("unchecked")
    private ConfigurationTransaction(T scope) throws Exception {
        if (scope.getType() == ConfigurationScope.Type.CUSTOMER_SPACE_SERVICE) {
            impl = (ConfigurationTransactionInterface<T>) new CustomerSpaceServiceConfigurationTransactionImpl(
                    (CustomerSpaceServiceScope) scope);
        }
        impl = new StandardConfigurationTransactionImpl<T>(scope);
    }

    public static <T extends ConfigurationScope> ConfigurationTransaction<T> construct(T scope) throws Exception {
        return new ConfigurationTransaction<T>(scope);
    }

    @Override
    public void check(Path path, Document document) throws Exception {
        impl.check(path, document);
    }

    @Override
    public void create(Path path, Document document) throws Exception {
        impl.create(path, document);
    }

    @Override
    public void set(Path path, Document document) throws Exception {
        impl.set(path, document);
    }

    @Override
    public void delete(Path path) throws Exception {
        impl.delete(path);
    }

    @Override
    public void commit() throws Exception {
        impl.commit();
    }

}

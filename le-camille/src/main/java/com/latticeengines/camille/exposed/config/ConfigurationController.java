package com.latticeengines.camille.exposed.config;

import java.util.AbstractMap;
import java.util.List;

import org.apache.curator.framework.api.CuratorWatcher;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;
import com.latticeengines.domain.exposed.camille.scopes.ServiceScope;

public class ConfigurationController<T extends ConfigurationScope> implements ConfigurationControllerInterface<T> {
    private ConfigurationControllerInterface<T> impl;

    @SuppressWarnings("unchecked")
    private ConfigurationController(T scope) throws Exception {
        if (scope.getType() == ConfigurationScope.Type.CUSTOMER_SPACE_SERVICE) {
            impl = (ConfigurationControllerInterface<T>) new CustomerSpaceServiceConfigurationControllerImpl(
                    (CustomerSpaceServiceScope) scope);
        } else if (scope.getType() == ConfigurationScope.Type.SERVICE) {
            impl = (ConfigurationControllerInterface<T>) new ServiceConfigurationControllerImpl((ServiceScope) scope);
        }
        impl = new StandardConfigurationControllerImpl<T>(scope);
    }

    public static <T extends ConfigurationScope> ConfigurationController<T> construct(T scope) {
        try {
            return new ConfigurationController<T>(scope);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void create(Path path, Document document) throws Exception {
        impl.create(path, document);
    }

    @Override
    public void upsert(Path path, Document document) throws Exception {
        impl.upsert(path, document);
    }

    @Override
    public void set(Path path, Document document) throws Exception {
        impl.set(path, document);
    }

    @Override
    public void set(Path path, Document document, boolean force) throws Exception {
        impl.set(path, document, force);
    }

    @Override
    public Document get(Path path) throws Exception {
        return impl.get(path);
    }

    @Override
    public Document get(Path path, CuratorWatcher watcher) throws Exception {
        return impl.get(path, watcher);
    }

    @Override
    public List<AbstractMap.SimpleEntry<Document, Path>> getChildren(Path path) throws Exception {
        return impl.getChildren(path);
    }

    @Override
    public DocumentDirectory getDirectory(Path path) throws Exception {
        return impl.getDirectory(path);
    }

    @Override
    public void delete(Path path) throws Exception {
        impl.delete(path);
    }

    @Override
    public boolean exists(Path path) throws Exception {
        return impl.exists(path);
    }
}

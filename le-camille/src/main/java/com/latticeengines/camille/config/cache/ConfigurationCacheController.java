package com.latticeengines.camille.config.cache;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.api.CuratorWatcher;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentHierarchy;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class ConfigurationCacheController<T extends ConfigurationScope> implements ConfigurationCacheControllerImpl<T> {

    private ConfigurationCacheControllerImpl<T> impl;

    public ConfigurationCacheController(T scope) {
        impl = ConfigurationCacheControllerImplFactory.getImplementation(scope);
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
    public List<Pair<Document, Path>> getChildren(Path path) throws Exception {
        return impl.getChildren(path);
    }

    @Override
    public DocumentHierarchy getHierarchy(Path path) throws Exception {
        return impl.getHierarchy(path);
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

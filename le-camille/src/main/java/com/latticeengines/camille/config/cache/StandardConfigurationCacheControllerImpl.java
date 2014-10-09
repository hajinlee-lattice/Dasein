package com.latticeengines.camille.config.cache;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.ZooDefs;

import com.latticeengines.camille.Camille;
import com.latticeengines.camille.CamilleEnvironment;
import com.latticeengines.camille.translators.PathTranslator;
import com.latticeengines.camille.translators.PathTranslatorFactory;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentHierarchy;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class StandardConfigurationCacheControllerImpl<T extends ConfigurationScope> implements
        ConfigurationCacheControllerImpl<T> {
    protected T scope;
    protected PathTranslator translator;
    protected Camille camille;

    public StandardConfigurationCacheControllerImpl(T scope) {
        this.scope = scope;
        this.translator = PathTranslatorFactory.getTranslator(scope);
        this.camille = CamilleEnvironment.getCamille();
    }

    @Override
    public void create(Path path, Document document) throws Exception {
        camille.create(translator.getAbsolutePath(path), document, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    @Override
    public void set(Path path, Document document) throws Exception {
        camille.set(translator.getAbsolutePath(path), document);
    }

    @Override
    public void set(Path path, Document document, boolean force) throws Exception {
        camille.set(translator.getAbsolutePath(path), document, force);
    }

    @Override
    public Document get(Path path) throws Exception {
        return camille.get(translator.getAbsolutePath(path));
    }

    @Override
    public Document get(Path path, CuratorWatcher watcher) throws Exception {
        return camille.get(translator.getAbsolutePath(path), watcher);
    }

    @Override
    public List<Pair<Document, Path>> getChildren(Path path) throws Exception {
        return camille.getChildren(translator.getAbsolutePath(path));
    }

    @Override
    public DocumentHierarchy getHierarchy(Path path) throws Exception {
        return camille.getHierarchy(translator.getAbsolutePath(path));
    }

    @Override
    public void delete(Path path) throws Exception {
        camille.delete(translator.getAbsolutePath(path));
    }

    @Override
    public boolean exists(Path path) throws Exception {
        return camille.exists(translator.getAbsolutePath(path));
    }
}

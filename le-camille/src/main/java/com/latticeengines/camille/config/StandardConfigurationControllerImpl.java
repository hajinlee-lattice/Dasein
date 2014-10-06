package com.latticeengines.camille.config;

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

public class StandardConfigurationControllerImpl<T extends ConfigurationScope> implements ConfigurationControllerImpl<T> {
    protected T scope;
    protected PathTranslator translator;
    protected Camille camille;

    public StandardConfigurationControllerImpl(T scope) {
        this.scope = scope;
        this.translator = PathTranslatorFactory.getTranslator(scope);
        this.camille = CamilleEnvironment.getCamille();
    }

    public void create(Path path, Document document) throws Exception {
        Path absolute = translator.getAbsolutePath(path);
        camille.create(absolute, document, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    public void set(Path path, Document document) throws Exception {
        Path absolute = translator.getAbsolutePath(path);
        camille.set(absolute, document);
    }

    public void set(Path path, Document document, boolean force) throws Exception {
        Path absolute = translator.getAbsolutePath(path);
        camille.set(absolute, document, force);
    }

    public Document get(Path path) throws Exception {
        Path absolute = translator.getAbsolutePath(path);
        return camille.get(absolute);
    }

    public Document get(Path path, CuratorWatcher watcher) throws Exception {
        Path absolute = translator.getAbsolutePath(path);
        return camille.get(absolute, watcher);
    }

    public List<Pair<Document, Path>> getChildren(Path path) throws Exception {
        Path absolute = translator.getAbsolutePath(path);
        return camille.getChildren(absolute);
    }

    public DocumentHierarchy getHierarchy(Path path) throws Exception {
        Path absolute = translator.getAbsolutePath(path);
        return camille.getHierarchy(absolute);
    }

    public void delete(Path path) throws Exception {
        Path absolute = translator.getAbsolutePath(path);
        camille.delete(absolute);
    }

    public boolean exists(Path path) throws Exception {
        Path absolute = translator.getAbsolutePath(path);
        return camille.exists(absolute);
    }

}

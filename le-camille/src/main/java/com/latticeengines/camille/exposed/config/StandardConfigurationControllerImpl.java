package com.latticeengines.camille.exposed.config;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.translators.PathTranslator;
import com.latticeengines.camille.exposed.translators.PathTranslatorFactory;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class StandardConfigurationControllerImpl<T extends ConfigurationScope> implements
        ConfigurationControllerInterface<T> {
    protected T scope;
    protected PathTranslator translator;
    protected Camille camille;

    public StandardConfigurationControllerImpl(T scope) {
        this.scope = scope;
        this.translator = PathTranslatorFactory.getTranslator(scope);
        this.camille = CamilleEnvironment.getCamille();
    }

    @Override
    public void create(Path path, Document document) throws Exception {
        Path absolute = translator.getAbsolutePath(path);
        Path base = translator.getBasePath();
        if (!camille.exists(base)) {
            throw new KeeperException.NoNodeException(base.toString());
        }
        camille.create(absolute, document, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    @Override
    public void upsert(Path path, Document document) throws Exception {
        Path absolute = translator.getAbsolutePath(path);
        camille.upsert(absolute, document, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    @Override
    public void set(Path path, Document document) throws Exception {
        Path absolute = translator.getAbsolutePath(path);
        camille.set(absolute, document);
    }

    @Override
    public void set(Path path, Document document, boolean force) throws Exception {
        Path absolute = translator.getAbsolutePath(path);
        camille.set(absolute, document, force);
    }

    @Override
    public Document get(Path path) throws Exception {
        Path absolute = translator.getAbsolutePath(path);
        return camille.get(absolute);
    }

    @Override
    public Document get(Path path, CuratorWatcher watcher) throws Exception {
        Path absolute = translator.getAbsolutePath(path);
        return camille.get(absolute, watcher);
    }

    @Override
    public List<Pair<Document, Path>> getChildren(Path path) throws Exception {
        // TODO all of this should really be handled in the controller itself
        Path absolute = translator.getAbsolutePath(path);
        List<Pair<Document, Path>> children = camille.getChildren(absolute);

        // Make paths local
        Iterator<Pair<Document, Path>> iter = children.iterator();
        while (iter.hasNext()) {
            Pair<Document, Path> pair = iter.next();
            pair.setValue(pair.getValue().local(path));
        }
        return children;
    }

    @Override
    public DocumentDirectory getDirectory(Path path) throws Exception {
        // TODO all of this should really be handled in the controller itself
        Path absolute = translator.getAbsolutePath(path);
        DocumentDirectory directory = camille.getDirectory(absolute);
        directory.makePathsLocal();
        return directory;
    }

    @Override
    public void delete(Path path) throws Exception {
        Path absolute = translator.getAbsolutePath(path);
        camille.delete(absolute);
    }

    @Override
    public boolean exists(Path path) throws Exception {
        Path absolute = translator.getAbsolutePath(path);
        return camille.exists(absolute);
    }
}

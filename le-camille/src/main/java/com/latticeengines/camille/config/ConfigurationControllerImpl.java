package com.latticeengines.camille.config;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.api.CuratorWatcher;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public interface ConfigurationControllerImpl<T extends ConfigurationScope> {
    public void create(Path path, Document document) throws Exception;

    public void set(Path path, Document document) throws Exception;

    public void set(Path path, Document document, boolean force) throws Exception;

    public Document get(Path path) throws Exception;

    public Document get(Path path, CuratorWatcher watcher) throws Exception;

    public List<Pair<Document, Path>> getChildren(Path path) throws Exception;

    public DocumentDirectory getDirectory(Path path) throws Exception;

    public void delete(Path path) throws Exception;

    public boolean exists(Path path) throws Exception;
}

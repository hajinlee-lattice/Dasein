package com.latticeengines.camille.exposed.config;

import java.util.AbstractMap;
import java.util.List;

import org.apache.curator.framework.api.CuratorWatcher;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public interface ConfigurationControllerInterface<T extends ConfigurationScope> {
    void create(Path path, Document document) throws Exception;

    void upsert(Path path, Document document) throws Exception;

    void set(Path path, Document document) throws Exception;

    void set(Path path, Document document, boolean force) throws Exception;

    Document get(Path path) throws Exception;

    Document get(Path path, CuratorWatcher watcher) throws Exception;

    List<AbstractMap.SimpleEntry<Document, Path>> getChildren(Path path) throws Exception;

    DocumentDirectory getDirectory(Path path) throws Exception;

    void delete(Path path) throws Exception;

    boolean exists(Path path) throws Exception;
}

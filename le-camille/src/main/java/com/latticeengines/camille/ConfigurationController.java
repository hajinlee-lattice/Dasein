package com.latticeengines.camille;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.api.CuratorWatcher;

import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentHierarchy;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public abstract class ConfigurationController {
    
    public abstract void create(Path path, Document document);
    public abstract void set(Path path, Document document);
    public abstract void set(Path path, Document document, boolean force);
    public abstract Document get(Path path);
    public abstract Document get(Path path, CuratorWatcher watcher);
    public abstract List<Pair<Document,Path>> getChildren(Path path);
    public abstract DocumentHierarchy getHierarchy(Path path);
    public abstract void delete(Path path);
    public abstract boolean exists(Path path);
}

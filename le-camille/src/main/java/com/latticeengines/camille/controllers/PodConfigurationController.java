package com.latticeengines.camille.controllers;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.api.CuratorWatcher;

import com.latticeengines.camille.ConfigurationController;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentHierarchy;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.PodScope;

public class PodConfigurationController extends ConfigurationController {
    private PodScope scope;

    public PodConfigurationController(PodScope scope) {
        this.scope = scope;
    }

    @Override
    public void create(Path path, Document document) {
        // TODO Auto-generated method stub

    }

    @Override
    public void set(Path path, Document document) {
        // TODO Auto-generated method stub

    }

    @Override
    public void set(Path path, Document document, boolean force) {
        // TODO Auto-generated method stub

    }

    @Override
    public Document get(Path path) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Document get(Path path, CuratorWatcher watcher) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Pair<Document, Path>> getChildren(Path path) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public DocumentHierarchy getHierarchy(Path path) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void delete(Path path) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean exists(Path path) {
        // TODO Auto-generated method stub
        return false;
    }

}

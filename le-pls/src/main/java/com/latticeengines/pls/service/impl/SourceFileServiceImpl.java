package com.latticeengines.pls.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;

@Component("sourceFileService")
public class SourceFileServiceImpl implements SourceFileService {
    @Autowired
    private SourceFileEntityMgr sourceFileEntityMgr;

    @Override
    public SourceFile findByName(String name) {
        return sourceFileEntityMgr.findByName(name);
    }

    @Override
    public void create(SourceFile sourceFile) {
        sourceFileEntityMgr.create(sourceFile);
    }

    @Override
    public void update(SourceFile sourceFile) {
        SourceFile existing = sourceFileEntityMgr.findByName(sourceFile.getName());
        if (existing != null) {
            delete(existing);
        }
        sourceFileEntityMgr.create(sourceFile);
    }

    @Override
    public void delete(SourceFile sourceFile) {
        sourceFileEntityMgr.delete(sourceFile);
    }

    @Override
    public SourceFile clone(String name) {
        return sourceFileEntityMgr.clone(name);
    }
}

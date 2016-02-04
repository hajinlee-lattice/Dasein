package com.latticeengines.workflow.exposed.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.workflow.exposed.entitymgr.SourceFileEntityMgr;
import com.latticeengines.workflow.exposed.service.SourceFileService;

@Component("sourceFileService")
public class SourceFileServiceImpl implements SourceFileService {
    @Autowired
    private SourceFileEntityMgr sourceFileEntityMgr;

    @Override
    public SourceFile findByName(String name) {
        return sourceFileEntityMgr.findByName(name);
    }
}

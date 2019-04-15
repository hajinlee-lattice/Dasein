package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.S3ImportSystemEntityMgr;
import com.latticeengines.apps.cdl.service.S3ImportSystemService;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;

@Component("s3ImportSystemService")
public class S3ImportSystemServiceImpl implements S3ImportSystemService {

    private static final Logger log = LoggerFactory.getLogger(S3ImportSystemServiceImpl.class);

    @Inject
    private S3ImportSystemEntityMgr s3ImportSystemEntityMgr;

    @Override
    public void createS3ImportSystem(String customerSpace, S3ImportSystem importSystem) {
        if (importSystem == null) {
            log.warn("Create NULL S3ImportSystem!");
            return;
        }
        if (s3ImportSystemEntityMgr.findS3ImportSystem(importSystem.getName()) != null) {
            throw new RuntimeException("Already have import system with name: " + importSystem.getName());
        }
        s3ImportSystemEntityMgr.createS3ImportSystem(importSystem);
    }

    @Override
    public S3ImportSystem getS3ImportSystem(String customerSpace, String name) {
        return s3ImportSystemEntityMgr.findS3ImportSystem(name);
    }
}

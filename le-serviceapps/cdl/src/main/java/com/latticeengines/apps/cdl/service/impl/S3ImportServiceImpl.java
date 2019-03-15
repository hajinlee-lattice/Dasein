package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.S3ImportMessageService;
import com.latticeengines.apps.cdl.service.S3ImportService;

@Component("s3ImportService")
public class S3ImportServiceImpl implements S3ImportService {

    @Inject
    private S3ImportMessageService s3ImportMessageService;

    @Override
    public boolean saveImportMessage(String bucket, String key) {
        return false;
    }

    @Override
    public boolean submitImportJob() {
        return false;
    }
}

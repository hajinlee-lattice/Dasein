package com.latticeengines.apps.cdl.service;

public interface S3ImportService {

    boolean saveImportMessage(String bucket, String key);

    boolean submitImportJob();
}

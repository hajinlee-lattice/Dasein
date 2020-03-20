package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.jms.S3ImportMessageType;

public interface S3ImportService {

    boolean saveImportMessage(String bucket, String key, String hostUrl, S3ImportMessageType messageType);

    boolean submitImportJob();
}

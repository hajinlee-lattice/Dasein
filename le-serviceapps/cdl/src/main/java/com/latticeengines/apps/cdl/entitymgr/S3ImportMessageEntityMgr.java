package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.S3ImportMessage;

public interface S3ImportMessageEntityMgr extends BaseEntityMgrRepository<S3ImportMessage, Long> {

    S3ImportMessage createS3ImportMessage(String bucket, String key);
}

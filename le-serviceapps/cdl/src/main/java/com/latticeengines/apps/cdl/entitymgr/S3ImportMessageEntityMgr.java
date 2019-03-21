package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.S3ImportMessage;

public interface S3ImportMessageEntityMgr extends BaseEntityMgrRepository<S3ImportMessage, Long> {

    S3ImportMessage createOrUpdateS3ImportMessage(String bucket, String key, String hostUrl);

    List<S3ImportMessage> getS3ImportMessageGroupByDropBox();
}

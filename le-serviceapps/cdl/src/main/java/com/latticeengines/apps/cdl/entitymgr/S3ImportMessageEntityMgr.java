package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.S3ImportMessage;
import com.latticeengines.domain.exposed.jms.S3ImportMessageType;

public interface S3ImportMessageEntityMgr extends BaseEntityMgrRepository<S3ImportMessage, Long> {

    S3ImportMessage createOrUpdateS3ImportMessage(String bucket, String key, S3ImportMessageType messageType);

    List<S3ImportMessage> getS3ImportMessageGroupByDropBox();

    void updateHostUrl(String key, String hostUrl);

    List<S3ImportMessage> getMessageWithoutHostUrlByType(S3ImportMessageType messageType);
}

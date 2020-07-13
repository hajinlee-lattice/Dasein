package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.S3ImportMessageEntityMgr;
import com.latticeengines.apps.cdl.service.S3ImportMessageService;
import com.latticeengines.domain.exposed.cdl.S3ImportMessage;
import com.latticeengines.domain.exposed.jms.S3ImportMessageType;

@Component("s3ImportMessageService")
public class S3ImportMessageServiceImpl implements S3ImportMessageService {

    @Inject
    private S3ImportMessageEntityMgr s3ImportMessageEntityMgr;

    @Override
    public S3ImportMessage createOrUpdateMessage(String bucket, String key, S3ImportMessageType messageType) {
        return s3ImportMessageEntityMgr.createOrUpdateS3ImportMessage(bucket, key, messageType);
    }

    @Override
    public List<S3ImportMessage> getMessageGroupByDropBox() {
        return s3ImportMessageEntityMgr.getS3ImportMessageGroupByDropBox();
    }

    @Override
    public void deleteMessage(S3ImportMessage message) {
        s3ImportMessageEntityMgr.delete(message);
    }

    @Override
    public List<S3ImportMessage> getMessageWithoutHostUrlByType(S3ImportMessageType messageType) {
        return s3ImportMessageEntityMgr.getMessageWithoutHostUrlByType(messageType);
    }

    @Override
    public void updateHostUrl(String key, String hostUrl) {
        s3ImportMessageEntityMgr.updateHostUrl(key, hostUrl);
    }
}

package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.S3ImportMessage;
import com.latticeengines.domain.exposed.jms.S3ImportMessageType;

public interface S3ImportMessageService {

    S3ImportMessage createOrUpdateMessage(String bucket, String key, S3ImportMessageType messageType);

    List<S3ImportMessage> getMessageGroupByDropBox();

    void deleteMessage(S3ImportMessage message);

    List<S3ImportMessage> getMessageWithoutHostUrlByType(S3ImportMessageType messageType);

    void updateHostUrl(String key, String hostUrl);
}

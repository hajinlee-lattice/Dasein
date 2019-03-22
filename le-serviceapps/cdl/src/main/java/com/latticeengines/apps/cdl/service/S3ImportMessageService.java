package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.S3ImportMessage;

public interface S3ImportMessageService {

    S3ImportMessage createOrUpdateMessage(String bucket, String key, String hostUrl);

    List<S3ImportMessage> getMessageGroupByDropBox();

    void deleteMessage(S3ImportMessage message);
}

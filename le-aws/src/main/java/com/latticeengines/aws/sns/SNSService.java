package com.latticeengines.aws.sns;

import java.util.Map;

import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishResult;

public interface SNSService {

    CreateTopicResult createTopic(String name);

    String getTopicArnByName(String name);

    PublishResult publishToTopic(String topicArn, String message,
            Map<String, MessageAttributeValue> messageAttributes) throws Exception;

    ListTopicsResult getAllTopics();

}

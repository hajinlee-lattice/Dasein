package com.latticeengines.aws.sns;

import java.util.Map;

import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishResult;

public interface SNSService {

    public CreateTopicResult createTopic(String name);

    public String getTopicArnByName(String name);

    public PublishResult publishToTopic(String topicArn, String message,
            Map<String, MessageAttributeValue> messageAttributes) throws Exception;

    public ListTopicsResult getAllTopics();

}

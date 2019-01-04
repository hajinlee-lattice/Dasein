package com.latticeengines.aws.sns;

import java.util.List;
import java.util.Map;

import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.PublishResult;

public interface SNSService {

    public CreateTopicResult createTopic(String name);

    public String getTopicArn(String name);

    public PublishResult publishToTopic(String topicArn, String message, Map<String, List<String>> messageAttributes) throws Exception;

    public ListTopicsResult getAllTopics();

}

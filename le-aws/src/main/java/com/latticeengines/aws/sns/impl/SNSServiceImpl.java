package com.latticeengines.aws.sns.impl;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
import com.amazonaws.services.sns.model.Topic;
import com.latticeengines.aws.sns.SNSService;
import com.latticeengines.common.exposed.util.JsonUtils;

@Component("snsService")
public class SNSServiceImpl implements SNSService {
    private static final Logger log = LoggerFactory.getLogger(SNSServiceImpl.class);

    @Resource(name = "awsCredentials")
    private AWSCredentials awsCredentials;

    private AmazonSNS snsClient;

    SNSServiceImpl(BasicAWSCredentials awsCredentials, @Value("${aws.region}") String region) {
        snsClient = AmazonSNSClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withRegion(region).build();
    }

    /* Returns existing topic if it already exists */
    @Override
    public CreateTopicResult createTopic(String name) {
        return snsClient.createTopic(name);
    }

    @Override
    public String getTopicArnByName(String name) {
        List<Topic> topics = getAllTopics().getTopics();
        topics = topics.stream().filter(topic -> topic.getTopicArn().endsWith(":" + name))
                .collect(Collectors.toList());
        return topics.size() != 1 ? null : topics.get(0).getTopicArn();
    }

    @Override
    public PublishResult publishToTopic(String topicName, String message,
            Map<String, MessageAttributeValue> messageAttributes) throws Exception {
        String topicArn = getTopicArnByName(topicName);
        if (StringUtils.isEmpty(topicArn) || StringUtils.isEmpty(message)) {
            throw new Exception(
                    String.format("TopicArn and/or message is invalid: %s, %s", topicArn, message));
        }

        PublishRequest publishRequest = new PublishRequest().withTopicArn(topicArn)
                .withMessage(message).withMessageAttributes(messageAttributes);

        log.info(String.format("Publishing message to TopicArn %s : ", topicArn)
                + JsonUtils.serialize(publishRequest));
        PublishResult publishResult = snsClient.publish(publishRequest);
        log.info(String.format("Published messageId %s", publishResult.getMessageId()));
        return publishResult;
    }

    @Override
    public PublishResult publishToTopic(String topicName, PublishRequest publishRequest) throws Exception {
        String topicArn = getTopicArnByName(topicName);
        if (StringUtils.isEmpty(topicArn)) {
            throw new Exception("Missing topicArn from AWS SNS PublishRequest");
        }
        
        publishRequest.setTopicArn(topicArn);

        log.info(String.format("Publishing message to TopicArn %s : ", topicArn) + JsonUtils.serialize(publishRequest));
        PublishResult publishResult = snsClient.publish(publishRequest);
        log.info(String.format("Published messageId %s", publishResult.getMessageId()));
        return publishResult;
    }

    @Override
    public ListTopicsResult getAllTopics() {
        return snsClient.listTopics();
    }
}

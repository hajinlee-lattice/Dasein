package com.latticeengines.aws.sns.impl;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.apache.commons.lang3.StringUtils;

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
    public String getTopicArn(String name) {
        return createTopic(name).getTopicArn();
    }

    @Override
    public PublishResult publishToTopic(String topicArn, String message,
            Map<String, List<String>> messageAttributes) throws Exception {
        if (StringUtils.isEmpty(topicArn) || topicArn == null) {
            throw new Exception("topicArn is invalid");
        } else if (StringUtils.isEmpty(message) || message == null) {
            throw new Exception("Message is invalid");
        }

        PublishRequest publishRequest = new PublishRequest().withTopicArn(topicArn)
                .withMessage(message);
        if (messageAttributes != null && !messageAttributes.isEmpty()) {
            Map<String, MessageAttributeValue> attributeValues = messageAttributes.entrySet()
                    .stream()
                    .collect(Collectors.toMap(entry -> entry.getKey(),
                            entry -> new MessageAttributeValue()
                                    .withDataType(entry.getValue().get(0))
                                    .withStringValue(entry.getValue().get(1))));
            publishRequest.withMessageAttributes(attributeValues);
        }

        log.info(String.format("Publishing message to TopicArn %s : ", topicArn)
                + JsonUtils.serialize(publishRequest));
        PublishResult publishResult = snsClient.publish(publishRequest);
        log.info(String.format("Published messageId %s", publishResult.getMessageId()));
        return publishResult;
    }

    @Override
    public ListTopicsResult getAllTopics() {
        return snsClient.listTopics();
    }
}

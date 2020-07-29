package com.latticeengines.datacloud.etl.publication.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.BasicAWSCredentials;
import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.aws.dynamo.impl.DynamoServiceImpl;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.datacloud.etl.publication.service.PublishConfigurationParser;
import com.latticeengines.domain.exposed.datacloud.publication.PublishToDynamoConfiguration;

@Component("publishConfigurationParser")
public class PublishConfigurationParserImpl implements PublishConfigurationParser {

    private static final Logger log = LoggerFactory.getLogger(PublishConfigurationParserImpl.class);

    @Value("${datacloud.aws.qa.access.key}")
    private String qaAwsAccessKey;

    @Value("${datacloud.aws.qa.secret.key}")
    private String qaAwsSecretKey;

    @Value("${datacloud.aws.prod.access.key}")
    private String prodAwsAccessKey;

    @Value("${datacloud.aws.prod.secret.key}")
    private String prodAwsSecretKey;

    @Value("${aws.region}")
    private String defaultAwsRegion;

    @Value("${aws.dynamo.customer.cmk}")
    private String customerCMK;

    @Inject
    private DynamoService defaultDynamoSerivce;

    @Override
    public PublishToDynamoConfiguration parseDynamoAlias(PublishToDynamoConfiguration dynamoConfiguration) {
        PublishToDynamoConfiguration.Alias alias = dynamoConfiguration.getAlias();
        if (alias != null) {
            switch (alias) {
            case QA:
                dynamoConfiguration.setAwsAccessKeyEncrypted(qaAwsAccessKey);
                dynamoConfiguration.setAwsSecretKeyEncrypted(qaAwsSecretKey);
                if (StringUtils.isBlank(dynamoConfiguration.getAwsRegion())) {
                    dynamoConfiguration.setAwsRegion(defaultAwsRegion);
                }
                break;
            case Production:
                dynamoConfiguration.setAwsAccessKeyEncrypted(prodAwsAccessKey);
                dynamoConfiguration.setAwsSecretKeyEncrypted(prodAwsSecretKey);
                if (StringUtils.isBlank(dynamoConfiguration.getAwsRegion())) {
                    dynamoConfiguration.setAwsRegion(defaultAwsRegion);
                }
                break;
            default:
                break;
            }
        }
        return dynamoConfiguration;
    }

    @Override
    public DynamoService constructDynamoService(PublishToDynamoConfiguration dynamoConfiguration) {
        String awsKeyEncrypted = dynamoConfiguration.getAwsAccessKeyEncrypted();
        String awsSecretEncrypted = dynamoConfiguration.getAwsSecretKeyEncrypted();
        String awsRegion = dynamoConfiguration.getAwsRegion();
        if (StringUtils.isNotBlank(awsKeyEncrypted) && StringUtils.isNotBlank(awsKeyEncrypted)) {
            if (StringUtils.isBlank(awsRegion)) {
                awsRegion = defaultAwsRegion;
            }
            log.info(String.format("Creating dynamo service using aws creds %s:%s (encrypted) in %s", awsKeyEncrypted,
                    awsSecretEncrypted, awsRegion));
            BasicAWSCredentials awsCredentials = new BasicAWSCredentials(CipherUtils.decrypt(awsKeyEncrypted),
                    CipherUtils.decrypt(awsSecretEncrypted));
            return new DynamoServiceImpl(awsCredentials, null, awsRegion, customerCMK);
        } else {
            log.info("aws creds parameters are not set, using default dynamo service.");
            return defaultDynamoSerivce;
        }
    }

}

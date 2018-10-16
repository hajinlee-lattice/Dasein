package com.latticeengines.apps.cdl.jms;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

@Component("s3ImportJmsConsumer")
public class S3ImportJmsConsumer {

    private static final Logger log = LoggerFactory.getLogger(S3ImportJmsConsumer.class);

    private static final String RECORDS = "Records";
    private static final String SNS = "Sns";
    private static final String MESSAGE = "Message";
    private static final String S3 = "s3";
    private static final String BUCKET = "bucket";
    private static final String NAME = "name";
    private static final String OBJECT = "object";
    private static final String KEY = "key";

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private DropBoxService dropBoxService;

    @JmsListener(destination = "${cdl.s3.file.import.sqs.name:}")
    public void processMessage(@Payload String message) {
        if (StringUtils.isEmpty(message)) {
            log.warn("S3 Import message is empty!");
            return;
        }
        log.info("Process message : " + message);
        submitImport(message);
    }

    private void submitImport(String message) {
        // TODO: this log is to be removed (YSong)
        log.info("Got message from SQS: " + message);
        JsonNode records = null;
        try {
            ObjectNode node = JsonUtils.deserialize(message, ObjectNode.class);
            records = node.get(RECORDS);
        } catch (Exception e) {
            log.error("Cannot deserialize message : " + message);
            return;
        }
        if (records != null && records.isArray()) {
            for (JsonNode record : records) {
                ObjectMapper om = new ObjectMapper();
                JsonNode innerRecords = null;
                try {
                    innerRecords = om.readTree(record.get(SNS).get(MESSAGE).asText());
                } catch (IOException e) {
                    log.error("Failed to parse inner records", e);
                }
                processS3Events(innerRecords);
            }
        }
    }

    private void processS3Events(JsonNode records) {
        if (records != null && records.isArray()) {
            for (JsonNode record : records) {
                JsonNode s3Node = record.get(S3);
                if (s3Node == null) {
                    return;
                }
                String bucket = s3Node.get(BUCKET).get(NAME).asText();
                String key = s3Node.get(OBJECT).get(KEY).asText();
                String[] parts = key.split("/");
                if (parts.length < 5) {
                    log.error("S3 import path is not correct!");
                    return;
                }
                String fileName = parts[parts.length - 1];
                String feedType = parts[parts.length - 2];
                String entity = parts[parts.length - 3];
                String dropBoxPrefix = parts[parts.length - 5];
                Tenant tenant = dropBoxService.getDropBoxOwner(dropBoxPrefix);
                if (tenant == null) {
                    log.error("Cannot find DropBox Owner: " + dropBoxPrefix);
                    return;
                }
                String tenantId = tenant.getId();
                tenantId = CustomerSpace.shortenCustomerSpace(tenantId);
                log.info(String.format("S3 import for %s / %s / %s / %s / %s", bucket, tenantId, entity, feedType,
                        fileName));
                submitApplication(tenantId, bucket, entity, feedType, key);
            }
        }
    }

    private void submitApplication(String tenantId, String bucket, String entity, String feedType, String key) {
        S3FileToHdfsConfiguration config = new S3FileToHdfsConfiguration();
        config.setEntity(BusinessEntity.getByName(entity));
        config.setFeedType(feedType);
        config.setS3Bucket(bucket);
        config.setS3FilePath(key);
        try {
            ApplicationId applicationId =  cdlProxy.submitS3ImportJob(tenantId, config);
            log.info("Start S3 file import by applicationId : " + applicationId.toString());
        } catch (Exception e) {
            log.error("Failed to submit s3 import job." + e.getMessage());
        }
    }
}

package com.latticeengines.apps.cdl.jms;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;

@Component("s3ImportJmsConsumer")
public class S3ImportJmsConsumer {

    private static final Logger log = LoggerFactory.getLogger(S3ImportJmsConsumer.class);

    private static final String RECORDS = "Records";
    private static final String MESSAGE = "Message";
    private static final String S3 = "s3";
    private static final String BUCKET = "bucket";
    private static final String NAME = "name";
    private static final String OBJECT = "object";
    private static final String KEY = "key";

    private static final String PS_SHARE = "PS_SHARE";

    private static final String STACK_INFO_URL = "/pls/health/stackinfo";

    private RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private BatonService batonService;

    @Inject
    private DropBoxService dropBoxService;

    @Value("${cdl.app.public.url:https://localhost:9081}")
    private String appPublicUrl;

    @Value("${common.le.stack}")
    private String currentStack;

    @JmsListener(destination = "${cdl.s3.file.import.sqs.name}")
    public void processMessage(@Payload String message) {
        if (StringUtils.isEmpty(message)) {
            log.warn("S3 Import message is empty!");
            return;
        }
        log.info("Process message : " + message);
        submitImport(message);
    }

    private void submitImport(String message) {
        JsonNode records;
        try {
            ObjectMapper om = new ObjectMapper();
            JsonNode node = om.readTree(message);
            records = om.readTree(node.get(MESSAGE).asText()).get(RECORDS);
        } catch (Exception e) {
            log.error("Cannot deserialize message : " + message);
            return;
        }
        if (records != null && records.isArray()) {
            for (JsonNode record : records) {
                JsonNode s3Node = record.get(S3);
                if (s3Node == null) {
                    return;
                }
                String bucket = s3Node.get(BUCKET).get(NAME).asText();
                String key = s3Node.get(OBJECT).get(KEY).asText();
                try {
                    key = URLDecoder.decode(key, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    log.error("Cannot decode object key " + key);
                }
                String[] parts = key.split("/");
                if (parts.length < 4 || !key.endsWith(".csv")) {
                    log.warn("S3 import path is not correct: " + key);
                    return;
                }
                String fileName = parts[parts.length - 1];
                String feedType = parts[parts.length - 2];
                if (PS_SHARE.equals(feedType)) {
                    // skip files in PS_SHARE folder.
                    return;
                }
                String dropBoxPrefix = parts[parts.length - 4];
                Tenant tenant = dropBoxService.getDropBoxOwner(dropBoxPrefix);
                if (tenant == null) {
                    log.error("Cannot find DropBox Owner: " + dropBoxPrefix);
                    return;
                }
                String tenantId = tenant.getId();
                tenantId = CustomerSpace.shortenCustomerSpace(tenantId);
                if (shouldRun(tenantId)) {
                    log.info(String.format("S3 import for %s / %s / %s / %s", bucket, tenantId, feedType, fileName));
                    submitApplication(tenantId, bucket, feedType, key);
                }
            }
        }
    }

    private void submitApplication(String tenantId, String bucket, String feedType, String key) {
        S3FileToHdfsConfiguration config = new S3FileToHdfsConfiguration();
        config.setFeedType(feedType);
        config.setS3Bucket(bucket);
        config.setS3FilePath(key);
        try {
            ApplicationId applicationId =  cdlProxy.submitS3ImportJob(tenantId, config);
            log.info("Start S3 file import by applicationId : " + applicationId.toString());
        } catch (Exception e) {
            log.error("Failed to submit s3 import job.", e);
        }
    }

    @PostConstruct
    private void setRestTemplate() {
        restTemplate.getInterceptors().add(new MagicAuthenticationHeaderHttpRequestInterceptor());
    }

    @SuppressWarnings("unchecked")
    private boolean shouldRun(String tenantId) {
        String url = appPublicUrl + STACK_INFO_URL;
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
        boolean currentActive = true;
        boolean inactiveImport = false;
        try {
            inactiveImport = batonService.isEnabled(customerSpace, LatticeFeatureFlag.AUTO_IMPORT_ON_INACTIVE);
        } catch (Exception e) {
            log.warn("Cannot get AUTO_IMPORT_ON_INACTIVE flag for " + tenantId + "default running on active stack");
        }
        try {
            Map<String, String> stackInfo = restTemplate.getForObject(url, Map.class);
            if (MapUtils.isNotEmpty(stackInfo) && stackInfo.containsKey("CurrentStack")) {
                String activeStack = stackInfo.get("CurrentStack");
                currentActive = currentStack.equalsIgnoreCase(activeStack);
                log.info("Current stack: " + currentStack + " Active stack: " + activeStack + " INACTIVE IMPORT=" +
                        String.valueOf(inactiveImport));
            }
            return currentActive ^ inactiveImport;
        } catch (Exception e) {
            // active stack is down, running on inactive
            log.warn("Cannot get active stack!", e);
            return inactiveImport;
        }
    }
}

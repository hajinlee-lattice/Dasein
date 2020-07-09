package com.latticeengines.apps.cdl.jms;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.apps.cdl.service.S3ImportService;
import com.latticeengines.apps.core.service.DropBoxService;
import com.latticeengines.apps.core.util.S3ImportMessageUtils;
import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.jms.S3ImportMessageType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;

@Component("s3ImportJmsConsumer")
public class S3ImportJmsConsumer {

    private static final Logger log = LoggerFactory.getLogger(S3ImportJmsConsumer.class);

    private static final String RECORDS = "Records";
    private static final String MESSAGE = "Message";
    private static final String MESSAGE_ID = "MessageId";
    private static final String S3 = "s3";
    private static final String BUCKET = "bucket";
    private static final String NAME = "name";
    private static final String OBJECT = "object";
    private static final String KEY = "key";
    private static final String REDIS_PREFIX = "S3ImportJmsConsumer_";

    private static Map<String, Integer> messageIdMap = new HashMap<>();
    private static Queue<String> messageIdQueue = new LinkedList<>();

    private RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    @Inject
    private RedisTemplate<String, Object> redisTemplate;

    @Inject
    private S3ImportService s3ImportService;

    @Inject
    private DropBoxService dropBoxService;

    @Value("${cdl.sqs.buffer.message.count:30000}")
    private int bufferedMessageIdCount;

    @Value("${cdl.sqs.key.idle.frame:300}")
    private int idleFrame;  //Same key in this time frame won't trigger import.

    @JmsListener(destination = "${cdl.s3.file.import.sqs.name}")
    public void processMessage(@Payload String message) {
        if (BeanFactoryEnvironment.Environment.WebApp.equals(BeanFactoryEnvironment.getEnvironment())) {
            if (StringUtils.isEmpty(message)) {
                log.warn("S3 Import message is empty!");
                return;
            }
            log.info("Process message : " + message);
            submitImport(message);
        }
    }

    private void submitImport(String message) {
        JsonNode records;
        try {
            ObjectMapper om = new ObjectMapper();
            JsonNode node = om.readTree(message);
            String messageId = node.get(MESSAGE_ID).asText();
            if (StringUtils.isEmpty(messageId)) {
                log.warn("Message Id is empty, skip import!");
                return;
            }
            synchronized (this) {
                if (messageIdMap.containsKey(messageId)) {
                    log.warn("Already processed message: " + messageId);
                    putMessageIdToBuffer(messageId);
                    return;
                } else {
                    putMessageIdToBuffer(messageId);
                }
            }
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
                S3ImportMessageType messageType = S3ImportMessageUtils.getMessageTypeFromKey(key);
                if (S3ImportMessageType.UNDEFINED.equals(messageType)) {
                    log.warn("S3 import path is not correct: " + key);
                    return;
                }
                String dropBoxPrefix = S3ImportMessageUtils.getDropBoxPrefix(key);
                Tenant tenant = dropBoxService.getDropBoxOwner(dropBoxPrefix);
                if (tenant == null) {
                    log.error("Cannot find DropBox Owner: " + dropBoxPrefix);
                    return;
                }
                if (S3ImportMessageUtils.shouldSkipMessage(key, messageType)) {
                    return;
                }
                String tenantId = tenant.getId();
                tenantId = CustomerSpace.shortenCustomerSpace(tenantId);
                synchronized (this) {
                    if (redisTemplate.opsForValue().get(REDIS_PREFIX + key) != null) {
                        log.warn(String.format("Already processed file %s in less then %d seconds, skip import!",
                                key, idleFrame));
                        return;
                    } else {
                        redisTemplate.opsForValue().set(REDIS_PREFIX + key, System.currentTimeMillis(),
                                idleFrame, TimeUnit.SECONDS);
                        log.info(String.format("S3 import for %s / %s / %s ", bucket, tenantId, key));
                        if (!s3ImportService.saveImportMessage(bucket, key, messageType)) {
                            log.warn(String.format("Cannot save import message: bucket %s, key %s",
                                    bucket, key));
                        }
                    }
                }
            }
        }
    }

    @PostConstruct
    private void initialize() {
        if (bufferedMessageIdCount <= 0) {
            bufferedMessageIdCount = 30000;
        }
        if (idleFrame <= 0) {
            idleFrame = 300;
        }
        restTemplate.getInterceptors().add(new MagicAuthenticationHeaderHttpRequestInterceptor());
    }

    private void putMessageIdToBuffer(String messageId) {
        while (messageIdQueue.size() >= bufferedMessageIdCount) {
            String popMessage = messageIdQueue.poll();
            if (messageIdMap.containsKey(popMessage)) {
                messageIdMap.put(popMessage, messageIdMap.get(popMessage) - 1);
                if (messageIdMap.get(popMessage) <= 0) {
                    messageIdMap.remove(popMessage);
                }
            }
        }
        messageIdQueue.offer(messageId);
        if (messageIdMap.containsKey(messageId)) {
            messageIdMap.put(messageId, messageIdMap.get(messageId) + 1);
        } else {
            messageIdMap.put(messageId, 1);
        }
    }
}

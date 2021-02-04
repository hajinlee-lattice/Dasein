package com.latticeengines.apps.cdl.jms;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
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
import com.latticeengines.apps.cdl.service.ImportMessageService;
import com.latticeengines.apps.cdl.service.S3ImportService;
import com.latticeengines.apps.core.service.DropBoxService;
import com.latticeengines.apps.core.util.S3ImportMessageUtils;
import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ImportMessage;
import com.latticeengines.domain.exposed.jms.S3ImportMessageType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.service.TenantService;

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

    private RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    @Inject
    private RedisTemplate<String, Object> redisTemplate;

    @Inject
    private S3ImportService s3ImportService;

    @Inject
    private TenantService tenantService;

    @Inject
    private ImportMessageService importMessageService;

    @Inject
    private DropBoxService dropBoxService;

    @Value("${cdl.sqs.buffer.message.count:30000}")
    private int bufferedMessageIdCount;

    @Value("${cdl.sqs.key.idle.frame:300}")
    private int idleFrame;  //Same key in this time frame won't trigger import.

    @Value("${cdl.s3.file.import.sqs.name}")
    private String s3ImportMessageQueue;

    @Value("${cdl.s3.inbound.connector.sqs.name}")
    private String inboundConnectionQueue;

    private static Map<String, Triple<Object, Map<String, Integer>, Queue<String>>> messageQueueMap = new HashMap<>();

    @PostConstruct
    public void initQueueMap() {
        initQueueMapByQueueName(s3ImportMessageQueue);
        initQueueMapByQueueName(inboundConnectionQueue);
    }

    private void initQueueMapByQueueName(String queueName) {
        Object lock = new Object();
        Map<String, Integer> messageIdMap = new HashMap<>();
        Queue<String> messageIdQueue = new LinkedList<>();
        messageQueueMap.put(queueName, Triple.of(lock, messageIdMap, messageIdQueue));
    }

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

    @JmsListener(destination = "${cdl.s3.inbound.connector.sqs.name}")
    public void processInboundConnectorMessage(@Payload String message) {
        if (StringUtils.isEmpty(message)) {
            log.warn("S3 inbound connector message is empty!");
            return;
        }
        log.info("Process inbound connector message : " + message);
        saveInboundConnectionMessage(message);
    }

    private void saveInboundConnectionMessage(String message) {
        String queueName = inboundConnectionQueue;
        JsonNode records = getRecords(queueName, message);
        if (records != null && records.isArray()) {
            for (JsonNode record : records) {
                JsonNode s3Node = record.get(S3);
                if (s3Node == null) {
                    return;
                }
                String key = getKey(s3Node);
                S3ImportMessageType messageType = S3ImportMessageUtils.getMessageTypeFromKey(key);
                // only handle mock broker instance for now
                if (S3ImportMessageType.INBOUND_CONNECTION.equals(messageType)) {
                    List<S3ImportMessageUtils.KeyPart> keyParts = new ArrayList<>();
                    keyParts.add(S3ImportMessageUtils.KeyPart.TENANT_ID);
                    keyParts.add(S3ImportMessageUtils.KeyPart.SOURCE_ID);
                    keyParts.add(S3ImportMessageUtils.KeyPart.FILE_NAME);
                    List<String> values = S3ImportMessageUtils.getKeyPartValues(key, messageType, keyParts);
                    String tenantId = values.get(0);
                    String sourceId = values.get(1);
                    String fileName = values.get(2);
                    if (StringUtils.isEmpty(tenantId) || StringUtils.isEmpty(sourceId) || StringUtils.isEmpty(fileName) || !fileName.endsWith(".csv")) {
                        log.info(String.format("key %s is invalid in inbound connection message, skip to save import message", key));
                        return;
                    }
                    String bucket = s3Node.get(BUCKET).get(NAME).asText();
                    Triple<Object, Map<String, Integer>, Queue<String>> triple = messageQueueMap.get(queueName);
                    Object lock = triple.getLeft();
                    synchronized (lock) {
                        log.info(String.format("Create or update import message for source %s with key %s.", sourceId, key));
                        importMessageService.createOrUpdateImportMessage(createImportMessage(bucket, key,
                                sourceId, messageType));
                    }
                }
            }
        }
    }

    private ImportMessage createImportMessage(String bucket, String key, String sourceId, S3ImportMessageType messageType) {
        ImportMessage importMessage = new ImportMessage();
        importMessage.setBucket(bucket);
        importMessage.setKey(key);
        importMessage.setSourceId(sourceId);
        importMessage.setMessageType(messageType);
        return importMessage;
    }

    private JsonNode getRecords(String queueName, String message) {
        try {
            ObjectMapper om = new ObjectMapper();
            JsonNode node = om.readTree(message);
            String messageId = node.get(MESSAGE_ID).asText();
            if (StringUtils.isEmpty(messageId)) {
                log.warn("Message Id is empty, skip import!");
                return null;
            }
            Triple<Object, Map<String, Integer>, Queue<String>> triple = messageQueueMap.get(queueName);
            Object lock = triple.getLeft();
            synchronized (lock) {
                Map<String, Integer> messageIdMap = triple.getMiddle();
                if (messageIdMap.containsKey(messageId)) {
                    log.warn("Already processed message: " + messageId);
                    putMessageIdToBuffer(queueName, messageId);
                    return null;
                } else {
                    putMessageIdToBuffer(queueName, messageId);
                }
            }
            return om.readTree(node.get(MESSAGE).asText()).get(RECORDS);
        } catch (Exception e) {
            log.error("Cannot deserialize message : " + message);
            return null;
        }
    }

    private String getKey(JsonNode s3Node) {
        String key = s3Node.get(OBJECT).get(KEY).asText();
        try {
            key = URLDecoder.decode(key, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            log.error("Cannot decode object key " + key);
        }
        return key;
    }

    private void submitImport(String message) {
        String queueName = s3ImportMessageQueue;
        JsonNode records = getRecords(queueName, message);
        if (records != null && records.isArray()) {
            for (JsonNode record : records) {
                JsonNode s3Node = record.get(S3);
                if (s3Node == null) {
                    return;
                }
                String bucket = s3Node.get(BUCKET).get(NAME).asText();
                String key = getKey(s3Node);
                String tenantId;
                S3ImportMessageType messageType = S3ImportMessageUtils.getMessageTypeFromKey(key);
                if (S3ImportMessageType.UNDEFINED.equals(messageType)) {
                    log.warn("S3 import path is not correct: " + key);
                    return;
                } else if (S3ImportMessageType.LISTSEGMENT.equals(messageType)) {
                    //set listsegment status to Pending
                    tenantId = S3ImportMessageUtils.getKeyPart(key, S3ImportMessageType.LISTSEGMENT,
                            S3ImportMessageUtils.KeyPart.TENANT_ID);
                    String segmentName = S3ImportMessageUtils.getKeyPart(key, S3ImportMessageType.LISTSEGMENT,
                            S3ImportMessageUtils.KeyPart.SEGMENT_NAME);
                } else {
                    String dropBoxPrefix = S3ImportMessageUtils.getDropBoxPrefix(key);
                    Tenant tenant = dropBoxService.getDropBoxOwner(dropBoxPrefix);
                    if (tenant == null) {
                        log.error("Cannot find DropBox Owner: " + dropBoxPrefix);
                        return;
                    }
                    tenantId = tenant.getId();
                    tenantId = CustomerSpace.shortenCustomerSpace(tenantId);
                }
                if (S3ImportMessageUtils.shouldSkipMessage(key, messageType)) {
                    return;
                }
                Triple<Object, Map<String, Integer>, Queue<String>> triple = messageQueueMap.get(queueName);
                Object lock = triple.getLeft();
                synchronized (lock) {
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

    private void putMessageIdToBuffer(String queueName, String messageId) {
        Triple<Object, Map<String, Integer>, Queue<String>> triple = messageQueueMap.get(queueName);
        Map<String, Integer> messageIdMap = triple.getMiddle();
        Queue<String> messageIdQueue = triple.getRight();
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

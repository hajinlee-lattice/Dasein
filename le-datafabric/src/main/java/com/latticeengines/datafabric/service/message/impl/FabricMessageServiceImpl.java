package com.latticeengines.datafabric.service.message.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.annotation.PostConstruct;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datafabric.RecordKey;
import com.latticeengines.domain.exposed.datafabric.TopicScope;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

@Component("messageService")
public class FabricMessageServiceImpl implements FabricMessageService {

    private static final Log log = LogFactory.getLog(FabricMessageServiceImpl.class);

    @Value("${datafabric.message.brokers}")
    private String brokers;

    @Value("${datafabric.message.zkConnect}")
    private String zkConnect;

    @Value("${datafabric.message.environment}")
    private String environment;

    @Value("${datafabric.message.stack}")
    private String stack;

    @Value("${datafabric.message.version}")
    private String version;

    @Value("${datafabric.message.schemaRegUrl}")
    private String schemaRegUrl;

    private Schema msgKeySchema;

    public FabricMessageServiceImpl(String brokers, String zkConnect, String schemaUrl, String stack, String environment) {
        this.brokers = brokers;
        this.zkConnect = zkConnect;
        this.schemaRegUrl = schemaUrl;
        this.version = "1.0.0";
        this.stack = stack;
        this.environment = environment;
        buildKeySchema();
    }

    public FabricMessageServiceImpl() {
    }

    @PostConstruct
    public void init() {
        log.info("Initialize message service with brokers " + brokers);
        buildKeySchema();
    }

    @Override
    public String getBrokers() {
        return brokers;
    }

    @Override
    public String getZkConnect() {
        return zkConnect;
    }

    @Override
    public String getSchemaRegUrl() {
        return schemaRegUrl;
    }

    @Override
    public String deriveTopic(String origTopic, TopicScope scope) {
        String topicEnvironment = environment;
        String topicStack = stack;

        switch (scope) {
        case PUBLIC:
            topicEnvironment = "global";
            topicStack = "global";
            break;
        case ENVIRONMENT_PRIVATE:
            topicStack = "global";
            break;
        case PRIVATE:
        default:
            break;
        }

        return String.format("Env_%s_Stack_%s_%s", topicEnvironment, topicStack, origTopic);
    }

    public GenericRecord buildKey(String producer, String recordType, String id) {

        GenericRecord key = new GenericData.Record(msgKeySchema);

        key.put("timeStamp", System.currentTimeMillis());
        key.put("version", this.version);
        key.put("producer", producer);
        key.put("record", recordType);
        key.put("id", id);
        key.put("customerSpace", "");

        return key;
    }

    public GenericRecord buildKey(RecordKey recordKey) {

        GenericRecord key = new GenericData.Record(msgKeySchema);
        // use reflection later
        key.put("id", recordKey.getId());
        key.put("customerSpace", recordKey.getCustomerSpace());
        key.put("timeStamp", recordKey.getTimeStamp());
        key.put("producer", recordKey.getProducer());
        key.put("record", recordKey.getRecordType());
        key.put("version", this.version);

        return key;
    }

    public boolean createTopic(String topic, TopicScope scope, int numPartitions, int numRepls) {

        boolean result = false;
        ZkClient zkClient = null;

        String derivedTopic = deriveTopic(topic, scope);

        try {
            zkClient = new ZkClient(zkConnect, 10000, 10000, ZKStringSerializer$.MODULE$);
            ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), false);

            if (!AdminUtils.topicExists(zkUtils, derivedTopic)) {
                AdminUtils.createTopic(zkUtils, derivedTopic, numPartitions, numRepls, new Properties(),
                        RackAwareMode.Enforced$.MODULE$);
                log.info("Topic created. name: " + topic + "partitions: " + numPartitions + "replications: " + numRepls);
            } else {
                log.info("Topic exists. name " + topic);
            }

            result = true;

        } catch (Exception ex) {
            log.error("Failed to create topic " + derivedTopic);
            log.error(ex);
        } finally {
            if (zkClient != null) {
                try {
                    zkClient.close();
                } catch (ZkInterruptedException e) {
                    log.error("Error when closing zk client.", e);
                }
            }
        }

        return result;
    }

    @Override
    public boolean deleteTopic(String topic, TopicScope scope) {

        boolean result = false;

        ZkClient zkClient = null;
        String derivedTopic = deriveTopic(topic, scope);

        try {
            zkClient = new ZkClient(zkConnect, 10000, 10000, ZKStringSerializer$.MODULE$);
            ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), false);

            if (AdminUtils.topicExists(zkUtils, derivedTopic)) {
                AdminUtils.deleteTopic(zkUtils, derivedTopic);
                log.info("Topic deleted. name: " + derivedTopic);
            } else {
                log.info("Topic does not exist. name: " + derivedTopic);
            }
            result = true;
        } catch (Exception ex) {
            log.error("Failed to delete topic " + derivedTopic);
            log.error(ex);
        } finally {
            if (zkClient != null) {
                try {
                    zkClient.close();
                } catch (ZkInterruptedException e) {
                    log.error("Error when closing zk client.", e);
                }
            }
        }

        return result;
    }

    private void buildKeySchema() {
        try {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("avro/MessageKey.avsc");
            Schema.Parser parser = new Schema.Parser();
            msgKeySchema = parser.parse(is);
        } catch (IOException e) {
            log.error("Message key schema avsc file not found", e);
            msgKeySchema = null;
        }
    }
}

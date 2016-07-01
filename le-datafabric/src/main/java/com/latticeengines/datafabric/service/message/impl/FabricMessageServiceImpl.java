package com.latticeengines.datafabric.service.message.impl;

import com.latticeengines.datafabric.service.message.FabricMessageService;

import java.io.InputStream;
import java.io.IOException;
import java.util.Properties;

import javax.annotation.PostConstruct;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("messageService")
public class FabricMessageServiceImpl implements FabricMessageService {

    private static final Log log = LogFactory.getLog(FabricMessageServiceImpl.class);

    @Value("${datafabric.message.brokers}")
    private String brokers;

    @Value("${datafabric.message.zkConnect}")
    private String zkConnect;

    @Value("${datafabric.message.stack}")
    private String stack;

    @Value("${datafabric.message.version}")
    private String version;

    @Value("${datafabric.message.schemaRegUrl}")
    private String schemaRegUrl;

    private Schema msgKeySchema;

    public FabricMessageServiceImpl(String brokers, String zkConnect, String schemaUrl, String stack) {
        this.brokers = brokers;
        this.zkConnect = zkConnect;
        this.schemaRegUrl = schemaUrl;
        this.version = "1.0.0";
        this.stack = stack;
        buildKeySchema();
    }

    public FabricMessageServiceImpl() {
    }

    @PostConstruct
    public void init() {
        log.info("Initialize message service with brokers " + brokers);
        buildKeySchema();
    }


    public String getBrokers() {
        return brokers;
    }

    public String getZkConnect() {
        return zkConnect;
    }

    public String getSchemaRegUrl() {
        return schemaRegUrl;
    }

    public String deriveTopic(String origTopic, boolean isGlobal) {
         String derivedTopic;

         if (isGlobal) {
             derivedTopic = "Stack_Global_" + origTopic;
         } else {
             derivedTopic = "Stack_" + stack + "_" + origTopic;
         }

         return derivedTopic;
    }

    public GenericRecord buildKey(String producer, String recordType, String id) {

          GenericRecord key = new GenericData.Record(msgKeySchema);

          key.put("timeStamp", System.currentTimeMillis());
          key.put("version", this.version);
          key.put("producer", producer);
          key.put("record", recordType);
          key.put("id", id);

          return key;
    }

    public boolean createTopic(String topic, boolean isShared, int numPartitions, int numRepls) {

        boolean result = false;
        ZkClient zkClient = null;
        ZkUtils zkUtils = null;

        String derivedTopic = deriveTopic(topic, isShared);

        try {
            zkClient = new ZkClient(zkConnect, 10000, 10000, ZKStringSerializer$.MODULE$);
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), false);
            Properties topicConfiguration = new Properties();

            if (!AdminUtils.topicExists(zkUtils, derivedTopic)) {
                AdminUtils.createTopic(zkUtils, derivedTopic, numPartitions, numRepls, new Properties(), RackAwareMode.Enforced$.MODULE$);
                log.info("Topic created. name: " + topic + "partitions: " + numPartitions +
                         "replications: " + numRepls);
            } else {
                log.info("Topic exisits. name " + topic);
            }

            result = true;

        } catch (Exception ex) {
            log.error("Failed to create topic " + derivedTopic); 
            log.error(ex);
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }

        return result;
    }

    public boolean deleteTopic(String topic, boolean shared) {

        boolean result = false;

        ZkClient zkClient = null;
        ZkUtils zkUtils = null;

        String derivedTopic = deriveTopic(topic, shared);

        try {
            zkClient = new ZkClient(zkConnect, 10000, 10000, ZKStringSerializer$.MODULE$);
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zkConnect), false);

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
                zkClient.close();
            }
        }

        return result;
    }

    private void buildKeySchema() {
         try {
             InputStream is = Thread.currentThread().getContextClassLoader()
                               .getResourceAsStream("avro/MessageKey.avsc");
             Schema.Parser parser = new Schema.Parser();
             msgKeySchema = parser.parse(is);
         } catch (IOException e) {
             log.error("Message key schema avsc file not found");
             log.error(e);
             msgKeySchema = null;
        }
    }
}

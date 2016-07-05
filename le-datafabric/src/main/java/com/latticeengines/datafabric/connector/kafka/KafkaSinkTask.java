package com.latticeengines.datafabric.connector.kafka;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.datafabric.service.message.FabricMessageProducer;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.datafabric.service.message.impl.FabricMessageProducerImpl;
import com.latticeengines.datafabric.service.message.impl.FabricMessageServiceImpl;
import com.latticeengines.domain.exposed.datafabric.TopicScope;

import io.confluent.connect.avro.AvroData;

public class KafkaSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(KafkaSinkTask.class);
    private AvroData avroData;
    private FabricMessageService messageService;
    private FabricMessageProducer producer;

    private String recordType;
    private String brokers;
    private String schemaRegUrl;
    private String zkConnect;
    private String stack;
    private String topic;
    private TopicScope scope;


    public KafkaSinkTask() {

    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        Set<TopicPartition> assignment = context.assignment();;

        try {
            KafkaSinkConnectorConfig connectorConfig = new KafkaSinkConnectorConfig(props);
            int schemaCacheSize = connectorConfig.getInt(KafkaSinkConnectorConfig.SCHEMA_CACHE_SIZE_CONFIG);
            avroData = new AvroData(schemaCacheSize);

            brokers = connectorConfig.getString(KafkaSinkConnectorConfig.KAFKA_BROKERS_CONFIG);
            zkConnect  = connectorConfig.getString(KafkaSinkConnectorConfig.KAFKA_ZKCONNECT_CONFIG);
            schemaRegUrl  = connectorConfig.getString(KafkaSinkConnectorConfig.KAFKA_SCHEMAREG_CONFIG);
            stack = connectorConfig.getString(KafkaSinkConnectorConfig.KAFKA_STACK_CONFIG);
            topic = connectorConfig.getString(KafkaSinkConnectorConfig.KAFKA_TOPIC_CONFIG);
            scope = TopicScope.fromName(connectorConfig.getString(KafkaSinkConnectorConfig.KAFKA_SCOPE_CONFIG));
            recordType = connectorConfig.getString(KafkaSinkConnectorConfig.KAFKA_RECORD_CONFIG);

            messageService = new FabricMessageServiceImpl(brokers,
                                                          zkConnect,
                                                          schemaRegUrl,
                                                          stack);


            log.info("Constructing producer for topic " + topic + "\n");

            producer = new FabricMessageProducerImpl(new FabricMessageProducerImpl.Builder(). //
                                                         messageService(messageService). //
                                                         topic(topic).scope(scope));

            // No recovery implemented assuming sinking to another Kafka cluster is idempotent.
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start KafkaSinkConnector due to configuration error.", e);
        } catch (ConnectException e) {
            log.error("Couldn't start KafkaSinkConnector:", e);
            log.error("Shutting down ReidsSinkConnector.");
        }
    }

    @Override
    public void stop() throws ConnectException {
    }

    @Override
    public void put(Collection<SinkRecord> records) throws ConnectException {

        Map<String, GenericRecord> avroRecords;

        avroRecords = new HashMap<String, GenericRecord>();

        try {
            for (SinkRecord record: records) {
                Struct value = (Struct)record.value();
                Struct key = (Struct)record.key();
                GenericRecord valueRec = (GenericRecord)avroData.fromConnectData(value.schema(), value);
                GenericRecord keyRec = (GenericRecord)avroData.fromConnectData(key.schema(), key);
                String id = keyRec.get("id").toString();;
                String recordType = keyRec.get("record").toString();;
                if (!recordType.equals(this.recordType))
                    continue;
                log.debug("Kafka connector sink record " + recordType + " " + this.recordType + " id " + id + "\n");
                producer.send(recordType, id, valueRec);
            }
        } catch (Exception e) {
            throw new ConnectException(e);
        }
        producer.flush();
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // Do nothing as the connector manages the offset
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        // Do nothing as the connector manages the offset
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        // Do nothing as the connector manages the offset
    }

    public AvroData getAvroData() {
        return avroData;
    }
}

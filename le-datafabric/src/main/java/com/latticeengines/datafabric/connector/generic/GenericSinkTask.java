package com.latticeengines.datafabric.connector.generic;

import io.confluent.connect.avro.AvroData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import com.latticeengines.datafabric.entitymanager.impl.GenericFabricEntityManagerImpl;
import com.latticeengines.datafabric.service.message.impl.FabricMessageServiceImpl;
import com.latticeengines.domain.exposed.datafabric.generic.GenericFabricRecord;

public class GenericSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(GenericSinkTask.class);
    private AvroData avroData;

    private GenericSinkConnectorConfig connectorConfig;
    private GenericFabricEntityManagerImpl<GenericFabricRecord> entityManager;

    public GenericSinkTask() {

    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            connectorConfig = new GenericSinkConnectorConfig(props);
            avroData = connectorConfig.constructAvroData();
            String pod = connectorConfig.getProperty(GenericSinkConnectorConfig.POD, String.class);
            String stack = connectorConfig.getProperty(GenericSinkConnectorConfig.STACK, String.class);
            String zkConnect = connectorConfig.getProperty(GenericSinkConnectorConfig.KAFKA_ZKCONNECT, String.class);
            FabricMessageServiceImpl messageService = new FabricMessageServiceImpl(pod, stack, zkConnect);
            entityManager = new GenericFabricEntityManagerImpl<GenericFabricRecord>();
            entityManager.setMessageService(messageService);

        } catch (Exception e) {
            throw new ConnectException("Couldn't start GenericConnector!", e);
        }
    }

    @Override
    public void stop() throws ConnectException {
    }

    @Override
    public void put(Collection<SinkRecord> records) throws ConnectException {

        if (records.size() <= 0) {
            log.info("There's no generic connector records.");
            return;
        }

        List<GenericRecord> keyRecords = new ArrayList<>();
        List<GenericRecord> valueRecords = new ArrayList<>();
        List<TopicPartition> topicPartitions = new ArrayList<>();
        try {
            for (SinkRecord record : records) {
                Struct key = (Struct) record.key();
                Struct value = (Struct) record.value();
                if (key == null || value == null)
                    continue;
                GenericRecord keyRec = (GenericRecord) avroData.fromConnectData(key.schema(), key);
                GenericRecord valueRec = (GenericRecord) avroData.fromConnectData(value.schema(), value);
                log.debug("Generic Connector Got key=" + keyRec.toString());
                log.debug("Generic Connector Got record=" + valueRec.toString());

                keyRecords.add(keyRec);
                valueRecords.add(valueRec);
                TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
                topicPartitions.add(tp);

            }
            log.info("Generic Connector batch size=" + keyRecords.size());
            GenericRecordProcessor processor = new GenericRecordProcessor(connectorConfig, entityManager);
            processor.addAll(keyRecords, valueRecords, topicPartitions);
            processor.process();

        } catch (Exception e) {
            log.error("Failed to execute connector tasks", e);
        }

    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        // Do nothing as the connector manages the offset
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        // Do nothing as the connector manages the offset
    }

}

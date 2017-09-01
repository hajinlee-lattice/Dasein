package com.latticeengines.datafabric.service.message.impl;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.datafabric.service.message.FabricMessageProducer;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datafabric.RecordKey;
import com.latticeengines.domain.exposed.datafabric.TopicScope;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;

public class FabricMessageProducerImpl implements FabricMessageProducer {

    private static final Logger log = LoggerFactory.getLogger(FabricMessageProducerImpl.class);

    private String producerName;

    private String topic;

    private String ackLevel;

    private int retries;

    private TopicScope scope;

    private KafkaProducer<Object, Object> producer;
    private Properties props;
    private String derivedTopic;

    @Autowired
    private FabricMessageService messageService;

    public FabricMessageProducerImpl(Builder builder) {
        this.topic = builder.topic;
        this.ackLevel = builder.ackLevel;
        this.retries = builder.retries;
        this.scope = builder.scope;
        this.producerName = builder.producer;
        if (builder.messageService != null)
            this.messageService = builder.messageService;

        createProducerInternal();
    }

    private void createProducerInternal() {
        props = new Properties();
        props.put("bootstrap.servers", messageService.getBrokers());
        props.put("schema.registry.url", messageService.getSchemaRegUrl());
        props.put("acks", ackLevel);
        props.put("retries", retries);
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");

        derivedTopic = messageService.deriveTopic(topic, scope);
        producer = new KafkaProducer<Object, Object>(props);
    }

    public Future<RecordMetadata> send(String recordType, String id, GenericRecord value) {

        GenericRecord key = messageService.buildKey(producerName, recordType, id);

        ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>(derivedTopic, key, value);

        log.debug("Send Message to " + derivedTopic);
        return producer.send(record);
    }

    public Future<RecordMetadata> send(RecordKey recordKey, GenericRecord value) {

        GenericRecord key = messageService.buildKey(recordKey);

        ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>(derivedTopic, key, value);

        log.debug("Send Message to " + derivedTopic);
        return producer.send(record);
    }

    @Override
    public Future<RecordMetadata> send(GenericRecordRequest recordRequest, GenericRecord value) {

        GenericRecord key = messageService.buildKey(recordRequest);

        ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>(derivedTopic, key, value);

        log.debug("Send Message to " + derivedTopic);
        return producer.send(record);
    }

    public void flush() {
        producer.flush();
    }

    public static class Builder {

        private String producer = "unknown";

        private String topic;

        private String ackLevel = "0";

        private int retries = 1;

        private TopicScope scope = TopicScope.PRIVATE;

        private FabricMessageService messageService = null;

        public Builder() {
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder ackLevel(String ackLevel) {
            this.ackLevel = ackLevel;
            return this;
        }

        public Builder retries(int retries) {
            this.retries = retries;
            return this;
        }

        public Builder scope(TopicScope scope) {
            this.scope = scope;
            return this;
        }

        public Builder producer(String producer) {
            this.producer = producer;
            return this;
        }

        public Builder messageService(FabricMessageService messageService) {
            this.messageService = messageService;
            return this;
        }
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}

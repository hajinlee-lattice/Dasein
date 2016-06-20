package com.latticeengines.datafabric.entitymanager.impl;

import java.lang.Class;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;


import com.latticeengines.datafabric.entitymanager.BaseFabricEntityMgr;
import com.latticeengines.datafabric.entitymanager.FabricEntityProcessor;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.datafabric.service.message.FabricMessageProducer;
import com.latticeengines.datafabric.service.message.FabricMessageConsumer;
import com.latticeengines.datafabric.service.message.impl.FabricMessageProducerImpl;
import com.latticeengines.datafabric.service.message.impl.SimpleFabricMessageConsumerImpl;
import com.latticeengines.datafabric.service.message.impl.FabricStreamProc;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;
import com.latticeengines.datafabric.util.RedisUtil;
import com.latticeengines.domain.exposed.dataplatform.HasId;

import javax.annotation.PostConstruct;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.annotation.Autowired;

public class BaseFabricEntityMgrImpl<T extends HasId<String>> implements BaseFabricEntityMgr<T> {

    private static final Log log = LogFactory.getLog(BaseFabricEntityMgrImpl.class);

    @Autowired
    private FabricMessageService messageService;

    @Autowired
    private FabricDataService dataService;

    private String store;

    private String repository;

    private String recordType;

    private String topic;

    private boolean sharedTopic;

    private FabricMessageProducer producer;

    private Map<String, Object> consumers;

    private Schema schema;

    private FabricDataStore dataStore;

    private Class<T> entityClass;

    public BaseFabricEntityMgrImpl(Builder builder) {

        this.store = builder.store;
        this.repository = builder.repository;
        this.recordType = builder.recordType;

        this.topic = builder.topic;
        this.sharedTopic = builder.sharedTopic;
        if (builder.messageService != null) this.messageService = builder.messageService;
        if (builder.dataService != null) this.dataService = builder.dataService;

        entityClass = getTypeParameterClass();
        // get the reflected schema for Entity
        schema = RedisUtil.getSchema(entityClass);

        dataStore = dataService.constructDataStore(store, repository, recordType, schema);

        if (topic != null) {
            producer = new FabricMessageProducerImpl(new FabricMessageProducerImpl.Builder().
                                                         messageService(this.messageService).
                                                         topic(this.topic).
                                                         shared(sharedTopic));

            consumers = new HashMap<String, Object>();
        }
    }

    @Override
    public void create(T entity) {
        GenericRecord record = entityToRecord(entity);
        dataStore.createRecord(entity.getId(), record);
    }

    @Override
    public void update(T entity) {
        GenericRecord record = entityToRecord(entity);
        dataStore.updateRecord(entity.getId(), record);
    }

    @Override
    public void delete(T entity) {
        GenericRecord record = entityToRecord(entity);
        dataStore.deleteRecord(entity.getId(), record);
    }

    public T findByKey(T entity) {
        GenericRecord record = dataStore.findRecord(entity.getId());
        return (record == null) ? null : recordToEntity(record);
    }

    public List<T> findByProperties(Map<String, String> properties) {
        List<GenericRecord> records = dataStore.findRecords(properties);
        List<T> entities = new ArrayList<T>();
        for (GenericRecord record : records) {
            entities.add(recordToEntity(record));
        }
        return entities;
    }

    @Override
    public void publish(T entity) {
        GenericRecord record = entityToRecord(entity);
        producer.send(recordType, entity.getId(), record);
    }

    public void addConsumer(String consumerGroup, FabricEntityProcessor processor, int numThreads) {

        FabricStreamProc streamProc = new BaseFabricEntityStreamProc(recordType, processor);

        FabricMessageConsumer consumer = new SimpleFabricMessageConsumerImpl(new SimpleFabricMessageConsumerImpl.Builder().
                                                                             messageService(messageService).
                                                                             group(consumerGroup).
                                                                             topic(topic).
                                                                             shared(sharedTopic).
                                                                             processor(streamProc).
                                                                             numThreads(numThreads));
        consumers.put(consumerGroup, consumer);
        log.info("Add consumer " + consumerGroup + " " + consumer.toString());
    }

    public void removeConsumer(String consumerGroup, int timeWaits) {
        FabricMessageConsumer consumer =  (FabricMessageConsumer)consumers.get(consumerGroup);
        if (consumer != null) {
            log.info("Remove consumer " + consumerGroup + " " + consumer + "\n");
        } else {
            log.info("Did not find consumer " + consumerGroup + " to delete\n");
        }
        consumer.stop(timeWaits);
        consumers.remove(consumerGroup);
    }

    private GenericRecord entityToRecord(T entity) {

        GenericRecord record  = null;
        ReflectDatumWriter<T> writer = new ReflectDatumWriter<T>(schema);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try {
            Encoder encoder = EncoderFactory.get().directBinaryEncoder(output, null);
            writer.write(entity, encoder);
            encoder.flush();

            ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
            Decoder decoder = DecoderFactory.get().directBinaryDecoder(input, null);
            record = reader.read(null, decoder);
        } catch (Exception e) {
            log.error("Failed to convert generic record to entity");
            log.error(e);
            return null;
        } finally {
            try { output.close(); } catch (Exception e) { }
        }

        return record;
    }

    private T recordToEntity(GenericRecord record) {

        T entity = null;

        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        ReflectDatumReader<T> reader = new ReflectDatumReader<T>(schema);
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        try {
            Encoder encoder = EncoderFactory.get().directBinaryEncoder(output, null);
            writer.write(record, encoder);
            encoder.flush();

            ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
            Decoder decoder = DecoderFactory.get().directBinaryDecoder(input, null);
            entity = reader.read(null, decoder);
        } catch (Exception e) {
            log.error("Failed to convert entity to generic record");
            log.error(e);
            return null;
        } finally {
            try { output.close(); } catch (Exception e) { }
        }

        return entity;
    }

    @SuppressWarnings("unchecked")
    private Class<T> getTypeParameterClass()
    {
        Type type = getClass().getGenericSuperclass();
        ParameterizedType paramType = (ParameterizedType) type;
        return (Class<T>) paramType.getActualTypeArguments()[0];
    }

    public class BaseFabricEntityStreamProc implements FabricStreamProc {

        FabricEntityProcessor processor;
        String recordType;

        public BaseFabricEntityStreamProc(String recordType, FabricEntityProcessor processor) {
            this.processor = processor;
            this.recordType = recordType;;
        }

        public void processRecord(String type, String id, GenericRecord record) {
            if (!type.equals(recordType)) {
                return;
            }
            T entity = recordToEntity(record);
            processor.process(entity);
        }
    }

    public static class Builder {

        private FabricMessageService messageService = null;
        private FabricDataService dataService = null;

        private String store;

        private String repository;

        private String recordType;

        private String topic;

        private boolean sharedTopic;

        public Builder store(String store) {
            this.store = store;
            return this;
        }


        public Builder repository(String repository) {
            this.repository = repository;
            return this;
        }
        public Builder recordType(String recordType) {
            this.recordType = recordType;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder sharedTopic(boolean sharedTopic) {
            this.sharedTopic = sharedTopic;
            return this;
        }
        public Builder messageService(FabricMessageService messageService) {
            this.messageService = messageService;
            return this;
        }

        public Builder dataService(FabricDataService dataService) {
            this.dataService = dataService;
            return this;
        }
    }
}

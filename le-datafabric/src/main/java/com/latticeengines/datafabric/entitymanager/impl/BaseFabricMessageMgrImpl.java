package com.latticeengines.datafabric.entitymanager.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import javax.annotation.PostConstruct;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.datafabric.entitymanager.BaseFabricMessageMgr;
import com.latticeengines.datafabric.entitymanager.FabricEntityProcessor;
import com.latticeengines.datafabric.service.message.FabricMessageConsumer;
import com.latticeengines.datafabric.service.message.FabricMessageProducer;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.datafabric.service.message.FabricStreamProc;
import com.latticeengines.datafabric.service.message.impl.FabricMessageProducerImpl;
import com.latticeengines.datafabric.service.message.impl.SimpleFabricMessageConsumerImpl;
import com.latticeengines.domain.exposed.datafabric.FabricEntityFactory;
import com.latticeengines.domain.exposed.datafabric.RecordKey;
import com.latticeengines.domain.exposed.datafabric.TopicScope;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;
import com.latticeengines.domain.exposed.dataplatform.HasId;

public class BaseFabricMessageMgrImpl<T extends HasId<String>> implements BaseFabricMessageMgr<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseFabricMessageMgrImpl.class);

    @Autowired
    protected FabricMessageService messageService;

    private String topic;

    private TopicScope scope;

    private FabricMessageProducer producer;

    private Map<String, Object> consumers;

    private Schema schema;
    private String recordType;
    private Class<T> entityClass;

    public BaseFabricMessageMgrImpl(Builder builder) {
        this.topic = builder.topic;
        this.scope = builder.scope;
        this.recordType = builder.recordType;

        if (builder.messageService != null) {
            this.messageService = builder.messageService;
        }
    }

    @Override
    @PostConstruct
    public void init() {
        entityClass = FabricEntityFactory.getTypeParameterClass(getClass().getGenericSuperclass());
        // get the reflected schema for Entity
        if (entityClass != null) {
            schema = FabricEntityFactory.getFabricSchema(entityClass, recordType);
        }
    }

    public void setMessageService(FabricMessageService messageService) {
        this.messageService = messageService;
    }

    @Override
    public Future<RecordMetadata> publish(GenericRecordRequest recordRequest, GenericRecord record) {
        if (record == null)
            return null;
        return getProducer().send(recordRequest, record);
    }

    @Override
    public void publish(T entity) {
        if (entity == null)
            return;
        try {
            Pair<GenericRecord, Map<String, Object>> pair = FabricEntityFactory.entityToPair(entity, recordType,
                    schema);
            GenericRecord record = (pair == null) ? null : pair.getLeft();
            getProducer().send(recordType, entity.getId(), record);
        } catch (Exception e) {
            log.info("Publish entity failed " + recordType + " " + entity.getId(), e);
        }
    }

    @Override
    public void publish(RecordKey recordKey, T entity) {
        if (entity == null)
            return;
        try {
            Pair<GenericRecord, Map<String, Object>> pair = FabricEntityFactory.entityToPair(entity, recordType,
                    schema);
            GenericRecord record = (pair == null) ? null : pair.getLeft();
            getProducer().send(recordKey, record);
        } catch (Exception e) {
            log.error("Publish entity failed " + recordType + " " + entity.getId(), e);
        }
    }

    @Override
    public void addConsumer(String consumerGroup, FabricEntityProcessor processor, int numThreads) {
        FabricStreamProc streamProc = new BaseFabricEntityStreamProc(recordType, processor);
        FabricMessageConsumer consumer = new SimpleFabricMessageConsumerImpl(
                new SimpleFabricMessageConsumerImpl.Builder().messageService(messageService).group(consumerGroup)
                        .topic(topic).scope(scope).processor(streamProc).numThreads(numThreads));
        consumers.put(consumerGroup, consumer);
        log.info("Add consumer " + consumerGroup + " " + consumer.toString());
    }

    @Override
    public void removeConsumer(String consumerGroup, int timeWaits) {
        FabricMessageConsumer consumer = (FabricMessageConsumer) consumers.get(consumerGroup);
        if (consumer != null) {
            log.info("Remove consumer " + consumerGroup + " " + consumer + "\n");
        } else {
            log.info("Did not find consumer " + consumerGroup + " to delete\n");
        }
        consumer.stop(timeWaits);
        consumers.remove(consumerGroup);
    }

    private FabricMessageProducer getProducer() {
        if (producer == null) {
            synchronized (this) {
                if (producer == null) {
                    log.info("Initializing Datafabric " + topic);
                    if (topic != null) {
                        producer = new FabricMessageProducerImpl(new FabricMessageProducerImpl.Builder()
                                .messageService(this.messageService).topic(this.topic).scope(scope));
                        consumers = new HashMap<>();
                    }
                }
            }
        }
        return producer;
    }

    public class BaseFabricEntityStreamProc implements FabricStreamProc {
        FabricEntityProcessor processor;
        String recordType;

        public BaseFabricEntityStreamProc(String recordType, FabricEntityProcessor processor) {
            this.processor = processor;
            this.recordType = recordType;
        }

        @Override
        public void processRecord(String type, String id, GenericRecord record) {
            if (!type.equals(recordType)) {
                return;
            }
            T entity = FabricEntityFactory.pairToEntity(Pair.of(record, null), entityClass, schema);
            processor.process(entity);
        }
    }

    @Override
    public String getRecordType() {
        return recordType;
    }

    @Override
    public void close() {
        if (producer != null)
            producer.close();
    }

    public static class Builder {
        private FabricMessageService messageService = null;
        private String recordType;
        private String topic;
        private TopicScope scope = TopicScope.PRIVATE;

        public Builder recordType(String recordType) {
            this.recordType = recordType;
            return this;
        }

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder scope(TopicScope scope) {
            this.scope = scope;
            return this;
        }

        public Builder messageService(FabricMessageService messageService) {
            this.messageService = messageService;
            return this;
        }
    }

}

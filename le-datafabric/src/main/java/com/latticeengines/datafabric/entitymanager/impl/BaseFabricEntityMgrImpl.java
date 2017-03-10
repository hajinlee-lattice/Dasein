package com.latticeengines.datafabric.entitymanager.impl;

import static com.latticeengines.datafabric.util.RedisUtil.INDEX;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.util.AvroReflectionUtils;
import com.latticeengines.datafabric.entitymanager.BaseFabricEntityMgr;
import com.latticeengines.datafabric.entitymanager.FabricEntityProcessor;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;
import com.latticeengines.datafabric.service.datastore.impl.DynamoDataStoreImpl;
import com.latticeengines.datafabric.service.message.FabricMessageConsumer;
import com.latticeengines.datafabric.service.message.FabricMessageProducer;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.datafabric.service.message.FabricStreamProc;
import com.latticeengines.datafabric.service.message.impl.FabricMessageProducerImpl;
import com.latticeengines.datafabric.service.message.impl.SimpleFabricMessageConsumerImpl;
import com.latticeengines.datafabric.util.DynamoUtil;
import com.latticeengines.datafabric.util.RedisUtil;
import com.latticeengines.domain.exposed.datafabric.DynamoIndex;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;
import com.latticeengines.domain.exposed.datafabric.FabricEntityFactory;
import com.latticeengines.domain.exposed.datafabric.RecordKey;
import com.latticeengines.domain.exposed.datafabric.TopicScope;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;
import com.latticeengines.domain.exposed.dataplatform.HasId;

public class BaseFabricEntityMgrImpl<T extends HasId<String>> implements BaseFabricEntityMgr<T> {

    private static final Log log = LogFactory.getLog(BaseFabricEntityMgrImpl.class);

    public static final String STORE_REDIS = "REDIS";
    public static final String STORE_DYNAMO = "DYNAMO";

    @Autowired
    protected FabricMessageService messageService;

    @Autowired
    protected FabricDataService dataService;

    private boolean disabled;

    private String store;

    private String topic;

    private TopicScope scope;

    private FabricMessageProducer producer;

    private Map<String, Object> consumers;

    private Schema schema;

    private String repository;

    private String recordType;

    protected FabricDataStore dataStore;

    private Class<T> entityClass;

    protected DynamoIndex tableIndex;

    private boolean enforceRemoteDynamo;

    private boolean initialized = false;

    public BaseFabricEntityMgrImpl(Builder builder) {
        this.store = builder.store;
        this.repository = builder.repository;
        this.recordType = builder.recordType;

        this.topic = builder.topic;
        this.scope = builder.scope;
        this.disabled = false;

        this.enforceRemoteDynamo = builder.enforceRemoteDynamo;

        if (builder.messageService != null) {
            this.messageService = builder.messageService;
        }
        if (builder.dataService != null) {
            this.dataService = builder.dataService;
        }
    }

    @Override
    @PostConstruct
    public void init() {
        log.info("Initializing Datafabric " + topic);
        if (disabled) {
            log.info("Datafabric disabled");
            return;
        }

        entityClass = getTypeParameterClass();
        // get the reflected schema for Entity
        if (entityClass != null) {
            schema = FabricEntityFactory.getFabricSchema(entityClass, recordType);
        }
        if (schema != null) {
            setupStore();
        }

        if (!disabled && (topic != null)) {
            producer = new FabricMessageProducerImpl(new FabricMessageProducerImpl.Builder()
                    .messageService(this.messageService).topic(this.topic).scope(scope));

            consumers = new HashMap<>();
        }
    }

    public void setMessageService(FabricMessageService messageService) {
        this.messageService = messageService;
    }

    private void setupStore() {
        // add redis index
        String redisIndex = RedisUtil.constructIndex(entityClass);
        schema.addProp(INDEX, redisIndex);

        // add dynamo key attributes
        String dynamoProp = DynamoUtil.constructIndex(entityClass);
        log.info("Index : " + dynamoProp);
        if (dynamoProp != null) {
            schema.addProp(DynamoUtil.KEYS, dynamoProp);
            tableIndex = DynamoUtil.getIndex(dynamoProp);
        }
        // add dynamo attributes
        dynamoProp = DynamoUtil.constructAttributes(entityClass);
        if (dynamoProp != null) {
            schema.addProp(DynamoUtil.ATTRIBUTES, dynamoProp);
        }

        if (store != null) {
            try {
                dataStore = dataService.constructDataStore(store, repository, recordType, schema);
                if (STORE_DYNAMO.endsWith(store)) {
                    ((DynamoDataStoreImpl) dataStore).useRemoteDynamo(enforceRemoteDynamo);
                }
            } catch (Exception e) {
                log.error("Failed to create data store " + store);
                disabled = true;
            }
        }
    }

    @Override
    public void create(T entity) {
        if (disabled) {
            return;
        }
        Pair<GenericRecord, Map<String, Object>> pair = entityToPair(entity);
        dataStore.createRecord(entity.getId(), pair);
    }

    @Override
    public void batchCreate(List<T> entities) {
        if (disabled) {
            return;
        }

        Map<String, Pair<GenericRecord, Map<String, Object>>> records = new HashMap<>();
        for (T entity : entities) {
            Pair<GenericRecord, Map<String, Object>> pair = entityToPair(entity);
            records.put(entity.getId(), pair);
        }
        dataStore.createRecords(records);
    }

    @Override
    public void update(T entity) {
        if (disabled) {
            return;
        }
        Pair<GenericRecord, Map<String, Object>> pair = entityToPair(entity);
        dataStore.updateRecord(entity.getId(), pair);
    }

    @Override
    public void delete(T entity) {
        if (disabled) {
            return;
        }
        Pair<GenericRecord, Map<String, Object>> pair = entityToPair(entity);
        GenericRecord record = (pair == null) ? null : pair.getLeft();
        dataStore.deleteRecord(entity.getId(), record);
    }

    @Override
    public T findByKey(T entity) {
        return findByKey(entity.getId());
    }

    @Override
    public T findByKey(String id) {
        if (disabled) {
            return null;
        }
        return pairToEntity(dataStore.findRecord(id));
    }

    @Override
    public List<T> batchFindByKey(List<String> ids) {
        if (disabled) {
            return null;
        }

        List<String> uniqueIds = dedupIds(ids);
        Map<String, Pair<GenericRecord, Map<String, Object>>> pairs = dataStore.batchFindRecord(uniqueIds);
        List<T> entities = new ArrayList<>();
        for (String id : ids) {
            Pair<GenericRecord, Map<String, Object>> pair = StringUtils.isEmpty(id) ? null : pairs.get(id);
            entities.add(pairToEntity(pair));
        }
        return entities;
    }

    @Override
    public List<T> findByProperties(Map<String, String> properties) {
        if (disabled)
            return null;
        List<Pair<GenericRecord, Map<String, Object>>> pairs = dataStore.findRecords(properties);
        List<T> entities = new ArrayList<T>();
        for (Pair<GenericRecord, Map<String, Object>> pair : pairs) {
            entities.add(pairToEntity(pair));
        }
        return entities;
    }

    @Override
    public void publish(T entity) {
        if (disabled || (entity == null))
            return;
        try {
            Pair<GenericRecord, Map<String, Object>> pair = entityToPair(entity);
            GenericRecord record = (pair == null) ? null : pair.getLeft();
            producer.send(recordType, entity.getId(), record);
        } catch (Exception e) {
            log.info("Publish entity failed " + recordType + " " + entity.getId(), e);
        }
    }

    @Override
    public void publish(RecordKey recordKey, T entity) {
        if (disabled || (entity == null))
            return;
        try {
            Pair<GenericRecord, Map<String, Object>> pair = entityToPair(entity);
            GenericRecord record = (pair == null) ? null : pair.getLeft();
            producer.send(recordKey, record);
        } catch (Exception e) {
            log.error("Publish entity failed " + recordType + " " + entity.getId(), e);
        }
    }

    @Override
    public void publish(GenericRecordRequest recordRequest, GenericRecord record) {
        if (disabled || record == null)
            return;
        Future<RecordMetadata> future = producer.send(recordRequest, record);
        try {
            if (!initialized) {
                future.get(5, TimeUnit.SECONDS);
            }
        } catch (Exception ex) {
            future.cancel(true);
            throw new RuntimeException("Publish timeout!", ex);
        } finally {
            initialized = true;
        }

    }

    @Override
    public void addConsumer(String consumerGroup, FabricEntityProcessor processor, int numThreads) {
        if (disabled)
            return;

        FabricStreamProc streamProc = new BaseFabricEntityStreamProc(recordType, processor);

        FabricMessageConsumer consumer = new SimpleFabricMessageConsumerImpl(
                new SimpleFabricMessageConsumerImpl.Builder().messageService(messageService).group(consumerGroup)
                        .topic(topic).scope(scope).processor(streamProc).numThreads(numThreads));
        consumers.put(consumerGroup, consumer);
        log.info("Add consumer " + consumerGroup + " " + consumer.toString());
    }

    @Override
    public void removeConsumer(String consumerGroup, int timeWaits) {
        if (disabled)
            return;
        FabricMessageConsumer consumer = (FabricMessageConsumer) consumers.get(consumerGroup);
        if (consumer != null) {
            log.info("Remove consumer " + consumerGroup + " " + consumer + "\n");
        } else {
            log.info("Did not find consumer " + consumerGroup + " to delete\n");
        }
        consumer.stop(timeWaits);
        consumers.remove(consumerGroup);
    }

    @Override
    public boolean isDisabled() {
        return disabled;
    }

    public void setIsDisabled(boolean disabled) {
        this.disabled = disabled;
    }

    protected Pair<GenericRecord, Map<String, Object>> entityToPair(T entity) {
        try {
            if (entity instanceof FabricEntity) {
                GenericRecord record = ((FabricEntity<?>) entity).toFabricAvroRecord(recordType);
                Map<String, Object> tags = ((FabricEntity<?>) entity).getTags();
                return Pair.of(record, tags);
            }
            log.info("Create Entity " + entity + "Schema " + schema.toString());
            return Pair.of(AvroReflectionUtils.toGenericRecord(entity, schema), null);
        } catch (Exception e) {
            log.error("Failed to convert entity to generic record", e);
            return null;
        }
    }

    protected Pair<GenericRecord, Map<String, Object>> entityToPair(T entity, Class<T> clazz) {
        try {
            if (entity instanceof FabricEntity) {
                GenericRecord record = ((FabricEntity<?>) entity).toFabricAvroRecord(recordType);
                Map<String, Object> tags = ((FabricEntity<?>) entity).getTags();
                return Pair.of(record, tags);
            }
            Schema schema = FabricEntityFactory.getFabricSchema(clazz, recordType);
            log.debug("Create Entity " + entity + "Schema " + schema.toString());
            return Pair.of(AvroReflectionUtils.toGenericRecord(entity, schema), null);
        } catch (Exception e) {
            log.error("Failed to convert entity to generic record", e);
            return null;
        }
    }

    private T pairToEntity(Pair<GenericRecord, Map<String, Object>> pair) {
        if (pair == null || pair.getLeft() == null) {
            return null;
        }
        GenericRecord record = pair.getLeft();
        Map<String, Object> tags = pair.getRight();
        T entity = FabricEntityFactory.fromFabricAvroRecord(record, entityClass, schema);
        FabricEntityFactory.setTags(entity, tags);
        return entity;
    }

    @SuppressWarnings("unchecked")
    private Class<T> getTypeParameterClass() {
        Type type = getClass().getGenericSuperclass();
        ParameterizedType paramType = (ParameterizedType) type;
        Type actualType = paramType.getActualTypeArguments()[0];
        if (actualType.getTypeName().equals("T")) {
            return null;
        }
        return (Class<T>) actualType;
    }

    private List<String> dedupIds(List<String> ids) {
        Set<String> uniqueIds = new HashSet<>();
        for (String id : ids) {
            if (StringUtils.isNotEmpty(id)) {
                uniqueIds.add(id);
            }
        }
        return new ArrayList<>(uniqueIds);
    }

    public class BaseFabricEntityStreamProc implements FabricStreamProc {

        FabricEntityProcessor processor;
        String recordType;

        public BaseFabricEntityStreamProc(String recordType, FabricEntityProcessor processor) {
            this.processor = processor;
            this.recordType = recordType;
            ;
        }

        @Override
        public void processRecord(String type, String id, GenericRecord record) {
            if (!type.equals(recordType)) {
                return;
            }
            T entity = pairToEntity(Pair.of(record, null));
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

        private TopicScope scope = TopicScope.PRIVATE;

        private boolean enforceRemoteDynamo = false;

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

        public Builder scope(TopicScope scope) {
            this.scope = scope;
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

        public Builder enforceRemoteDynamo(boolean enforce) {
            this.enforceRemoteDynamo = enforce;
            return this;
        }
    }

    @Override
    public String getRepository() {
        return repository;
    }

    @Override
    public String getRecordType() {
        return recordType;
    }

    @Override
    public Map<String, Object> findAttributesByKey(String id) {
        return dataStore.findAttributes(id);
    }

}

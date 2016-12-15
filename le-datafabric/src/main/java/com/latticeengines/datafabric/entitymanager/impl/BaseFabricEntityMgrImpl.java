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

import javax.annotation.PostConstruct;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.common.exposed.util.AvroReflectionUtils;
import com.latticeengines.datafabric.entitymanager.BaseFabricEntityMgr;
import com.latticeengines.datafabric.entitymanager.FabricEntityProcessor;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;
import com.latticeengines.datafabric.service.message.FabricMessageConsumer;
import com.latticeengines.datafabric.service.message.FabricMessageProducer;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.datafabric.service.message.FabricStreamProc;
import com.latticeengines.datafabric.service.message.impl.FabricMessageProducerImpl;
import com.latticeengines.datafabric.service.message.impl.SimpleFabricMessageConsumerImpl;
import com.latticeengines.datafabric.util.DynamoUtil;
import com.latticeengines.datafabric.util.RedisUtil;
import com.latticeengines.domain.exposed.datafabric.FabricEntity;
import com.latticeengines.domain.exposed.datafabric.FabricEntityFactory;
import com.latticeengines.domain.exposed.datafabric.RecordKey;
import com.latticeengines.domain.exposed.datafabric.TopicScope;
import com.latticeengines.domain.exposed.datafabric.DynamoIndex;
import com.latticeengines.domain.exposed.dataplatform.HasId;

public class BaseFabricEntityMgrImpl<T extends HasId<String>> implements BaseFabricEntityMgr<T> {

    private static final Log log = LogFactory.getLog(BaseFabricEntityMgrImpl.class);

    @Autowired
    private FabricMessageService messageService;

    @Autowired
    private FabricDataService dataService;

    @Value("${datafabric.disabled:false}")
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

    public BaseFabricEntityMgrImpl(Builder builder) {
        this.store = builder.store;
        this.repository = builder.repository;
        this.recordType = builder.recordType;

        this.topic = builder.topic;
        this.scope = builder.scope;
        this.disabled = false;

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
        schema = FabricEntityFactory.getFabricSchema(entityClass, recordType);

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

        dataStore = dataService.constructDataStore(store, repository, recordType, schema);

        if (topic != null) {
            producer = new FabricMessageProducerImpl(new FabricMessageProducerImpl.Builder()
                    .messageService(this.messageService).topic(this.topic).scope(scope));

            consumers = new HashMap<>();
        }
    }

    @Override
    public void create(T entity) {
        if (disabled) {
            return;
        }
        GenericRecord record = entityToRecord(entity);
        dataStore.createRecord(entity.getId(), record);
    }

    @Override
    public void batchCreate(List<T> entities) {
        if (disabled) {
            return;
        }

        Map<String, GenericRecord> records = new HashMap<>();
        for (T entity : entities) {
            GenericRecord record = entityToRecord(entity);
            records.put(entity.getId(), record);
        }
        dataStore.createRecords(records);
    }

    @Override
    public void update(T entity) {
        if (disabled) {
            return;
        }
        GenericRecord record = entityToRecord(entity);
        dataStore.updateRecord(entity.getId(), record);
    }

    @Override
    public void delete(T entity) {
        if (disabled) {
            return;
        }
        GenericRecord record = entityToRecord(entity);
        dataStore.deleteRecord(entity.getId(), record);
    }

    @Override
    public T findByKey(T entity) {
        if (disabled) {
            return null;
        }
        GenericRecord record = dataStore.findRecord(entity.getId());
        return (record == null) ? null : recordToEntity(record);
    }

    @Override
    public T findByKey(String id) {
        if (disabled) {
            return null;
        }
        GenericRecord record = dataStore.findRecord(id);
        return (record == null) ? null : recordToEntity(record);
    }

    @Override
    public List<T> batchFindByKey(List<String> ids) {
        if (disabled) {
            return null;
        }

        List<String> uniqueIds = dedupIds(ids);
        Map<String, GenericRecord> records = new HashMap<>();
        records = dataStore.batchFindRecord(uniqueIds);
        List<T> entities = new ArrayList<>();
        for (String id : ids) {
            GenericRecord record = StringUtils.isEmpty(id) ? null : records.get(id);
            entities.add((record == null) ? null : recordToEntity(record));
        }
        return entities;
    }

    @Override
    public List<T> findByProperties(Map<String, String> properties) {
        if (disabled)
            return null;
        List<GenericRecord> records = dataStore.findRecords(properties);
        List<T> entities = new ArrayList<T>();
        for (GenericRecord record : records) {
            entities.add(recordToEntity(record));
        }
        return entities;
    }

    @Override
    public void publish(T entity) {
        if (disabled || (entity == null))
            return;
        try {
            GenericRecord record = entityToRecord(entity);
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
            GenericRecord record = entityToRecord(entity);
            producer.send(recordKey, record);
        } catch (Exception e) {
            log.info("Publish entity failed " + recordType + " " + entity.getId());
            log.info(e);
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

    private GenericRecord entityToRecord(T entity) {

        try {
            if (entity instanceof FabricEntity) {
                return ((FabricEntity<?>) entity).toFabricAvroRecord(recordType);
            }
            log.info("Create Entity " + entity + "Schema " + schema.toString());
            return AvroReflectionUtils.toGenericRecord(entity, schema);
        } catch (Exception e) {
            log.error("Failed to convert entity to generic record", e);
            return null;
        }
    }

    private T recordToEntity(GenericRecord record) {
        return FabricEntityFactory.fromFabricAvroRecord(record, entityClass, schema);
    }

    @SuppressWarnings("unchecked")
    private Class<T> getTypeParameterClass() {
        Type type = getClass().getGenericSuperclass();
        ParameterizedType paramType = (ParameterizedType) type;
        return (Class<T>) paramType.getActualTypeArguments()[0];
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

        private TopicScope scope = TopicScope.PRIVATE;

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

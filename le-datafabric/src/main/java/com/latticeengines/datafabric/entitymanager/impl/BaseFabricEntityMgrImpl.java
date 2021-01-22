package com.latticeengines.datafabric.entitymanager.impl;

import static com.latticeengines.datafabric.util.RedisUtil.INDEX;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.datafabric.entitymanager.BaseFabricEntityMgr;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;
import com.latticeengines.datafabric.service.datastore.impl.DynamoDataStoreImpl;
import com.latticeengines.datafabric.util.DynamoUtil;
import com.latticeengines.datafabric.util.RedisUtil;
import com.latticeengines.domain.exposed.datafabric.DynamoIndex;
import com.latticeengines.domain.exposed.datafabric.FabricEntityFactory;
import com.latticeengines.domain.exposed.dataplatform.HasId;
public class BaseFabricEntityMgrImpl<T extends HasId<String>> implements BaseFabricEntityMgr<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseFabricEntityMgrImpl.class);

    public static final String STORE_DYNAMO = "DYNAMO";

    @Inject
    protected FabricDataService dataService;

    private boolean disabled;

    private String store;

    private Schema schema;

    private String repository;

    private String recordType;

    protected FabricDataStore dataStore;

    private Class<T> entityClass;

    protected DynamoIndex tableIndex;

    private boolean enforceRemoteDynamo;

    public BaseFabricEntityMgrImpl(Builder builder) {
        this.store = builder.store;
        this.repository = builder.repository;
        this.recordType = builder.recordType;

        this.disabled = false;

        this.enforceRemoteDynamo = builder.enforceRemoteDynamo;

        if (builder.dataService != null) {
            this.dataService = builder.dataService;
        }
    }

    @Override
    @PostConstruct
    public void init() {
        if (disabled) {
            log.info("Datafabric disabled");
            return;
        }

        entityClass = FabricEntityFactory.getTypeParameterClass(getClass().getGenericSuperclass());
        // get the reflected schema for Entity
        if (entityClass != null) {
            schema = FabricEntityFactory.getFabricSchema(entityClass, recordType);
        }
        if (schema != null) {
            setupStore();
        }
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
                if (STORE_DYNAMO.equalsIgnoreCase(store)) {
                    ((DynamoDataStoreImpl) dataStore).useRemoteDynamo(enforceRemoteDynamo);
                }
            } catch (Exception e) {
                log.error("Failed to create data store " + store, e);
                disabled = true;
            }
        }
    }

    @Override
    public void create(T entity) {
        if (disabled) {
            return;
        }
        Pair<GenericRecord, Map<String, Object>> pair = FabricEntityFactory.entityToPair(entity, recordType, schema);
        dataStore.createRecord(entity.getId(), pair);
    }

    @Override
    public void batchCreate(List<T> entities) {
        if (disabled) {
            return;
        }

        Map<String, Pair<GenericRecord, Map<String, Object>>> records = new HashMap<>();
        for (T entity : entities) {
            Pair<GenericRecord, Map<String, Object>> pair = FabricEntityFactory.entityToPair(entity, recordType,
                    schema);
            records.put(entity.getId(), pair);
        }
        dataStore.createRecords(records);
    }

    @Override
    public void update(T entity) {
        if (disabled) {
            return;
        }
        Pair<GenericRecord, Map<String, Object>> pair = FabricEntityFactory.entityToPair(entity, recordType, schema);
        dataStore.updateRecord(entity.getId(), pair);
    }

    @Override
    public void delete(T entity) {
        if (disabled) {
            return;
        }
        Pair<GenericRecord, Map<String, Object>> pair = FabricEntityFactory.entityToPair(entity, recordType, schema);
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
        return FabricEntityFactory.pairToEntity(dataStore.findRecord(id), entityClass, schema);
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
            entities.add(FabricEntityFactory.pairToEntity(pair, entityClass, schema));
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
            entities.add(FabricEntityFactory.pairToEntity(pair, entityClass, schema));
        }
        return entities;
    }

    @Override
    public boolean isDisabled() {
        return disabled;
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

    public static class Builder {
        private FabricDataService dataService = null;
        private String store;
        private String repository;
        private String recordType;
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

        public Builder dataService(FabricDataService dataService) {
            this.dataService = dataService;
            return this;
        }

        public Builder enforceRemoteDynamo(boolean enforce) {
            this.enforceRemoteDynamo = enforce;
            return this;
        }
    }

}

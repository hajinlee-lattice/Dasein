package com.latticeengines.datafabric.entitymanager.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datafabric.entitymanager.BaseFabricEntityMgr;
import com.latticeengines.datafabric.entitymanager.GenericTableEntityMgr;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.domain.exposed.datafabric.GenericTableEntity;

public class GenericTableEntityMgrImpl implements GenericTableEntityMgr {

    private final GenericTableInternalEntityMgr internalEntityMgr;

    public GenericTableEntityMgrImpl(FabricDataService dataService,
            String signature) {
        internalEntityMgr = new GenericTableInternalEntityMgrImpl(dataService, signature);
        internalEntityMgr.init();
    }

    @Override
    public Map<String, Object> getByKeyPair(String tenantId, String tableName, Pair<String, String> keyPair) {
        String id = concatenateKeys(tenantId, tableName, keyPair);
        Map<String, Object> result = null;
        if (id != null) {
            GenericTableEntity entity = internalEntityMgr.findByKey(id);
            if (entity != null) {
                result = entity.getAttributes();
            }
        }
        return result;
    }

    @Override
    public Map<String, Object> getByKeyPair(Map<String, List<String>> tenantIdsAndTableNames,
            Pair<String, String> keyPair) {
        Map<String, Object> result = null;
        List<String> ids = new ArrayList<>();
        for (Map.Entry<String, List<String>> ent : tenantIdsAndTableNames.entrySet()) {
            List<String> parts = ent.getValue().stream()
                    .map(tableName -> concatenateKeys(ent.getKey(), tableName, keyPair)).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(parts)) {
                ids.addAll(parts);
            }
        }
        if (CollectionUtils.isNotEmpty(ids)) {
            List<GenericTableEntity> entities = internalEntityMgr.batchFindByKey(ids);
            for (GenericTableEntity entity : entities) {
                if (entity != null) {
                    if (result == null) {
                        result = new HashMap<>();
                    }
                    result.putAll(entity.getAttributes());
                }
            }
        }
        return result;
    }

    @Override
    public List<Map<String, Object>> getByKeyPairs(String tenantId, String tableName,
            List<Pair<String, String>> keyPairs) {
        List<Map<String, Object>> results = new ArrayList<>();
        List<String> ids = keyPairs.stream().map(pair -> concatenateKeys(tenantId, tableName, pair))
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(ids)) {
            List<GenericTableEntity> entities = internalEntityMgr.batchFindByKey(ids);
            entities.forEach(entity -> {
                if (entity == null) {
                    results.add(null);
                } else {
                    results.add(entity.getAttributes());
                }
            });
        }
        return results;
    }

    @Override
    public List<Map<String, Object>> getAllByPartitionKey(String tenantId, String tableName, String partitionKeyValue) {
        String partitionKey = String.format("%s_%s_%s", tenantId, tableName, partitionKeyValue);
        Map<String, String> properties = new HashMap<>(); //
        properties.put("PartitionKey", partitionKey);
        List<GenericTableEntity> entities = internalEntityMgr.findByProperties(properties);
        List<Map<String, Object>> results = new ArrayList<>();
        entities.forEach(entity -> {
            System.out.println(JsonUtils.serialize(entity));
            if (entity == null) {
                results.add(null);
            } else {
                results.add(entity.getAttributes());
            }
        });
        return results;
    }

    private String concatenateKeys(String tenantId, String tableName, Pair<String, String> keyPair) {
        String concatenatedKey = "";
        if (keyPair != null) {
            String partitionKey = keyPair.getLeft();
            String sortKey = keyPair.getRight();
            if (StringUtils.isNotBlank(partitionKey)) {
                if (StringUtils.isBlank(sortKey)) {
                    sortKey = GenericTableEntity.DUMMY_SORT_KEY;
                }
                concatenatedKey = String.format("%s_%s_%s#%s", tenantId, tableName, partitionKey, sortKey);
            }
        }
        return concatenatedKey;
    }

    private interface GenericTableInternalEntityMgr extends BaseFabricEntityMgr<GenericTableEntity> {
    }

    private class GenericTableInternalEntityMgrImpl extends BaseFabricEntityMgrImpl<GenericTableEntity>
            implements GenericTableInternalEntityMgr {
        GenericTableInternalEntityMgrImpl(FabricDataService dataService,
                String signature) {
            super(new BaseFabricEntityMgrImpl.Builder() //
                    .dataService(dataService) //
                    .recordType(GenericTableEntity.class.getSimpleName() + "_" + signature) //
                    .store(BaseFabricEntityMgrImpl.STORE_DYNAMO) //
                    .enforceRemoteDynamo(true) //
                    .repository("GenericTable"));
        }
    }

}

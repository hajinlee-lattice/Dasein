package com.latticeengines.datafabric.entitymanager.impl;

import java.util.ArrayList;
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
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datafabric.GenericTableEntity;

public class GenericTableEntityMgrImpl implements GenericTableEntityMgr {

    private final GenericTableInternalEntityMgr internalEntityMgr;

    public GenericTableEntityMgrImpl(FabricMessageService messageService, FabricDataService dataService,
            String signature) {
        internalEntityMgr = new GenericTableInternalEntityMgrImpl(messageService, dataService, signature);
        internalEntityMgr.init();
    }

    @Override
    public Map<String, Object> getByKeyPair(String tenantId, String tableName, Pair<String, String> keyPair) {
        String id = contactKeys(tenantId, tableName, keyPair);
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
    public List<Map<String, Object>> getByKeyPairs(String tenantId, String tableName,
            List<Pair<String, String>> keyPairs) {
        List<Map<String, Object>> results = new ArrayList<>();
        List<String> ids = keyPairs.stream().map(pair -> contactKeys(tenantId, tableName, pair))
                .collect(Collectors.toList());
        System.out.println("ids = " + ids);
        if (CollectionUtils.isNotEmpty(ids)) {
            List<GenericTableEntity> entities = internalEntityMgr.batchFindByKey(ids);
            entities.forEach(entity -> {
                System.out.println(JsonUtils.serialize(entity));
                if (entity == null) {
                    results.add(null);
                } else {
                    results.add(entity.getAttributes());
                }
            });
        }
        return results;
    }

    private String contactKeys(String tenantId, String tableName, Pair<String, String> keyPair) {
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
        GenericTableInternalEntityMgrImpl(FabricMessageService messageService, FabricDataService dataService,
                String signature) {
            super(new BaseFabricEntityMgrImpl.Builder() //
                    .messageService(messageService) //
                    .dataService(dataService) //
                    .recordType(GenericTableEntity.class.getSimpleName() + "_" + signature) //
                    .store(BaseFabricEntityMgrImpl.STORE_DYNAMO) //
                    .enforceRemoteDynamo(true) //
                    .repository("GenericTable"));
        }
    }

}

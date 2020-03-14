package com.latticeengines.datafabric.entitymanager;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

public interface GenericTableEntityMgr {

    Map<String, Object> getByKeyPair(String tenantId, String tableName, Pair<String, String> keyPair);

    Map<String, Object> getByKeyPair(Map<String, List<String>> tenantIdsAndTableNames, Pair<String, String> keyPair);

    List<Map<String, Object>> getByKeyPairs(String tenantId, String tableName, List<Pair<String, String>> keyPairs);

    List<Map<String, Object>> getAllByPartitionKey(String tenantId, String tableName, String partitionKey);
}

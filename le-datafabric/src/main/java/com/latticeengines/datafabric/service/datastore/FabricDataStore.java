package com.latticeengines.datafabric.service.datastore;

import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;

public interface FabricDataStore {

    void createRecord(String id, Pair<GenericRecord, Map<String, Object>> pair);

    void updateRecord(String id, Pair<GenericRecord, Map<String, Object>> pair);

    void createRecords(Map<String, Pair<GenericRecord, Map<String, Object>>> pairs);

    Pair<GenericRecord, Map<String, Object>> findRecord(String id);

    Map<String, Pair<GenericRecord, Map<String, Object>>> batchFindRecord(List<String> idList);

    List<Pair<GenericRecord, Map<String, Object>>> findRecords(Map<String, String> properties);

    void deleteRecord(String id, GenericRecord record);

    Map<String, Object> findAttributes(String id);

}

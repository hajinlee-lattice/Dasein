package com.latticeengines.datafabric.service.datastore;

import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;

public interface FabricDataStore {

    void createRecord(String id, GenericRecord record);
    void updateRecord(String id, GenericRecord record);

    void createRecords(Map<String, GenericRecord> records);

    GenericRecord findRecord(String id);

    List<GenericRecord> findRecords(Map<String, String> properties);

    void deleteRecord(String id, GenericRecord record);

}


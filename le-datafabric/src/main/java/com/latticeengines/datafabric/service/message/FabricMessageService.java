package com.latticeengines.datafabric.service.message;

import org.apache.avro.generic.GenericRecord;

import com.latticeengines.domain.exposed.datafabric.RecordKey;
import com.latticeengines.domain.exposed.datafabric.TopicScope;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;

public interface FabricMessageService {

    String getBrokers();

    String getZkConnect();

    String getSchemaRegUrl();

    String deriveTopic(String topic, TopicScope scope);

    GenericRecord buildKey(String producer, String recordType, String id);

    GenericRecord buildKey(GenericRecordRequest recordRequest);

    boolean createTopic(String topic, TopicScope scope, int numPartitions, int numRepls);

    boolean deleteTopic(String topic, TopicScope scope);

    GenericRecord buildKey(RecordKey recordKey);

    boolean createZNode(String entityName, String data, boolean createNew);

    String readData(String entityName);

    void incrementalUpdate(String entityName, DataUpdater<String> updater);

    boolean cleanup(String entityName);

}

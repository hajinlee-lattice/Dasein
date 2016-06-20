package com.latticeengines.datafabric.service.message;

import org.apache.avro.generic.GenericRecord;

public interface FabricMessageService {

    String getBrokers();

    String getZkConnect();

    String getSchemaRegUrl();

    String deriveTopic(String topic, boolean isShared);

    GenericRecord buildKey(String producer, String recordType, String id);

    boolean createTopic(String topic, boolean shared, int numPartitions, int numRepls);

    boolean deleteTopic(String topic, boolean shared);
}



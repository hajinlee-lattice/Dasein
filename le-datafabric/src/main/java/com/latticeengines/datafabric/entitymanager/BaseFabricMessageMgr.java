package com.latticeengines.datafabric.entitymanager;

import java.util.concurrent.Future;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.latticeengines.domain.exposed.datafabric.RecordKey;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;

public interface BaseFabricMessageMgr<T> {

    void publish(T entity);

    void publish(RecordKey recordKey, T entity);

    Future<RecordMetadata> publish(GenericRecordRequest recordRequest, GenericRecord record);

    void addConsumer(String processorName, FabricEntityProcessor proc, int threadNumber);

    void removeConsumer(String processorName, int waitTime);

    String getRecordType();

    void init();

    void close();
}

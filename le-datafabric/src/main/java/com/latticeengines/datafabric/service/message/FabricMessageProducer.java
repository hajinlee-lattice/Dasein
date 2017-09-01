package com.latticeengines.datafabric.service.message;

import java.util.concurrent.Future;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.latticeengines.domain.exposed.datafabric.RecordKey;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;

public interface FabricMessageProducer {

    Future<RecordMetadata> send(String recordType, String id, GenericRecord record);

    void flush();

    Future<RecordMetadata> send(RecordKey recordKey, GenericRecord record);

    Future<RecordMetadata> send(GenericRecordRequest recordRequest, GenericRecord value);

    void close();

}

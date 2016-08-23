package com.latticeengines.datafabric.service.message;

import java.util.concurrent.Future;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.latticeengines.domain.exposed.datafabric.RecordKey;

public interface FabricMessageProducer {

    Future<RecordMetadata> send(String recordType, String id, GenericRecord record);

    void flush();

    Future<RecordMetadata> send(RecordKey recordKey, GenericRecord record);

}

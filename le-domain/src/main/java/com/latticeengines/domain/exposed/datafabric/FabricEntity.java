package com.latticeengines.domain.exposed.datafabric;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public interface FabricEntity<T> {

    GenericRecord toAvroRecord(String recordType);

    Schema getSchema(String recordType);

    //TODO: can change to static method in Java 8
    T fromAvroRecord(GenericRecord record);

}

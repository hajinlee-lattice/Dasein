package com.latticeengines.domain.exposed.datafabric;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.latticeengines.domain.exposed.dataplatform.HasId;

public interface FabricEntity<T> extends HasId<String> {

    GenericRecord toFabricAvroRecord(String recordType);

    Schema getSchema(String recordType);

    //TODO: can change to static method in Java 8
    T fromFabricAvroRecord(GenericRecord record);

    //TODO: can change to static method in Java 8
    T fromHdfsAvroRecord(GenericRecord record);

}

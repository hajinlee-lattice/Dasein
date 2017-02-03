package com.latticeengines.domain.exposed.datafabric;

import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.latticeengines.domain.exposed.dataplatform.HasId;

public interface FabricEntity<T> extends HasId<String> {

    GenericRecord toFabricAvroRecord(String recordType);

    Schema getSchema(String recordType);

    T fromFabricAvroRecord(GenericRecord record);

    T fromHdfsAvroRecord(GenericRecord record);

    <U> U getTag(String tagName, Class<U> tagClass);

    void setTag(String tagName, Object value);

    Map<String, Object> getTags();

}

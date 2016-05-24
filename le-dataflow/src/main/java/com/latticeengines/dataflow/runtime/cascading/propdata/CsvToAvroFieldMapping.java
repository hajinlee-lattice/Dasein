package com.latticeengines.dataflow.runtime.cascading.propdata;

import org.apache.avro.Schema;

import cascading.tuple.coerce.Coercions.Coerce;

public interface CsvToAvroFieldMapping {
    String getAvroFieldName(String csvFieldName);

    String getCsvFieldName(String avroFieldName);

    Coerce<?> getFieldType(String csvFieldName);

    Schema getAvroSchema();

}

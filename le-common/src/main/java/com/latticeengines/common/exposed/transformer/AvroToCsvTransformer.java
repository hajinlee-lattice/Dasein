package com.latticeengines.common.exposed.transformer;

import java.util.List;
import java.util.function.Function;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public interface AvroToCsvTransformer {
    public List<String> getFieldNames(Schema schema);

    public Function<GenericRecord, List<String[]>> getCsvConverterFunction();
}

package com.latticeengines.common.exposed.transformer;

import java.util.List;
import java.util.function.Function;

import org.apache.avro.generic.GenericRecord;

public interface AvroToCsvTransformer {

    List<String> getFieldNames();

    Function<GenericRecord, List<String[]>> getCsvConverterFunction();

}

package com.latticeengines.eai.routes.converter;

import org.apache.avro.Schema.Type;

public interface AvroTypeConverter {

	Type convertTypeToAvro(String type);
	
}

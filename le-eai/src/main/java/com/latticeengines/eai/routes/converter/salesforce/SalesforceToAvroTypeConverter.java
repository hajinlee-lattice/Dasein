package com.latticeengines.eai.routes.converter.salesforce;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema.Type;
import org.springframework.stereotype.Component;

import com.latticeengines.eai.routes.converter.AvroTypeConverter;

@Component("salesforceToAvroTypeConverter")
public class SalesforceToAvroTypeConverter implements AvroTypeConverter {

	private Map<String, Type> typeMap = new HashMap<String, Type>();
	
	public SalesforceToAvroTypeConverter() {
		typeMap.put("string", Type.STRING);
		typeMap.put("email", Type.STRING);
		typeMap.put("phone", Type.STRING);
		typeMap.put("url", Type.STRING);
		typeMap.put("textarea", Type.STRING);
		typeMap.put("id", Type.STRING);
		typeMap.put("picklist", Type.ENUM);
		typeMap.put("int", Type.INT);
		typeMap.put("currency", Type.DOUBLE);
		typeMap.put("percent", Type.DOUBLE);
		typeMap.put("boolean", Type.BOOLEAN);
		typeMap.put("date", Type.STRING);
		typeMap.put("datetime", Type.STRING);
	}
	
	@Override
	public Type convertTypeToAvro(String type) {
		return typeMap.get(type);
	}

}

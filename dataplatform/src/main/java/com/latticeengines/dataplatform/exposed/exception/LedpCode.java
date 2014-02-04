package com.latticeengines.dataplatform.exposed.exception;

public enum LedpCode {
	LEDP_10000("Metadata schema is null."),
	LEDP_10001("Metadata schema is not retrievable from hdfs."),
	LEDP_10002("At least one feature required."),
	LEDP_10003("Exactly one target required."),
	LEDP_10004("Feature {0} not found in schema."),
	LEDP_10005("Could not deserialize data schema.");
	
	private String message;
	
	LedpCode(String message) {
		this.message = message;
	}
	
	public String getMessage() {
		return message;
	}
}

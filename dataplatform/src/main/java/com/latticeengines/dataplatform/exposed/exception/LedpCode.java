package com.latticeengines.dataplatform.exposed.exception;

public enum LedpCode {
	// Validation service: 10000-10999
	LEDP_10000("Metadata schema is null."), //
	LEDP_10001("Metadata schema is not retrievable from hdfs."), //
	LEDP_10002("At least one feature required."), //
	LEDP_10003("Exactly one target required."), //
	LEDP_10004("Feature {0} not found in schema."), //
	LEDP_10005("Could not deserialize data schema."), //
	// Metadata service: 11000-11999
	LEDP_11000("Could not load driver class {0}."), //
	LEDP_11001("Failed connecting to db."), //
	LEDP_11002("Issue running query {0}."), //
	LEDP_11003("Unsupported type {0} for determining default value.");
	
	private String message;
	
	LedpCode(String message) {
		this.message = message;
	}
	
	public String getMessage() {
		return message;
	}
}

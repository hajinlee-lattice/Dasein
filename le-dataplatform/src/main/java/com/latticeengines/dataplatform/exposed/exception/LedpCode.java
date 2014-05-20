package com.latticeengines.dataplatform.exposed.exception;

public enum LedpCode {
    // Low level errors: 00000-09999
    LEDP_00000("Could not create hdfs dir {0}."), //
    LEDP_00001("Could not collect yarn queue information from ResourceManager."), //
    LEDP_00002("Generic system error."), //
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
    LEDP_11003("Could not retrieve keys for table {0}."), //
    // Runtime service: 12000-12999
    LEDP_12000("Parameter PRIORITY undefined for analytics job."), //
    LEDP_12003("No avro file in specified directory {0}."), //
    LEDP_12004("Sampling configuration property ledp.sampling.config not specified."), //
    LEDP_12005("Model definition must not be null."), //
    LEDP_12006("Parameter QUEUE undefined for analytics job."), //
    LEDP_12007("Parameter CUSTOMER undefined for analytics job."), //
    // Metric system: 13000-13999
    LEDP_13000("Tag {0} does not have a value."), //
    // Persistence service: 14000-14999
    LEDP_14000("Could not create configuration store {0}."), //
    LEDP_14001("Could not load configuration store {0}."), //
    LEDP_14002("Could not save configuration store {0}."), //
    // Modeling service: 15000-15999
    LEDP_15000("Could not create model schema."), //
    LEDP_15001("Could not find sample file for prefix {0}.");

    private String message;

    LedpCode(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}

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
    LEDP_10006("Name is null."), //
    // Metadata service: 11000-11999
    LEDP_11000("Could not load driver class {0}."), //
    LEDP_11001("Failed connecting to db."), //
    LEDP_11002("Issue running query {0}."), //
    LEDP_11003("Could not retrieve keys for table {0}."), //
    // Runtime service: 12000-12999
    LEDP_12000("Parameter PRIORITY undefined for analytics job."), //
    LEDP_12001("Could not create runtime configuration."), //
    LEDP_12002("Could not retrieve application id for load job {0}."), //
    LEDP_12003("No avro file in specified directory {0}."), //
    LEDP_12004("Sampling configuration property ledp.sampling.config not specified."), //
    LEDP_12005("Model definition must not be null."), //
    LEDP_12006("Parameter QUEUE undefined for analytics job."), //
    LEDP_12007("Parameter CUSTOMER undefined for analytics job."), //
    LEDP_12008("Table {0} does not exist for analytics load job."), //
    // Metric system: 13000-13999
    LEDP_13000("Tag {0} does not have a value."), //
    // Persistence service: 14000-14999
    LEDP_14000("Could not create configuration store {0}."), //
    LEDP_14001("Could not load configuration store {0}."), //
    LEDP_14002("Could not save configuration store {0}."), //
    // Modeling service: 15000-15999
    LEDP_15000("Could not create model schema."), //
    LEDP_15001("Could not find sample file for prefix {0}."), //
    LEDP_15002("Customer must be set for a model."), //
    LEDP_15003("There should at least be one file of type {0}."), //
    LEDP_15004("Could not find diagnostics for model input data."), //
    LEDP_15005("Input data has too few rows: {0}."), //
    LEDP_15006("Failed to validate the input data."), //
    // DLOrchestration service: 16000-16999
    LEDP_16000("Missing model command parameter(s) {0}."), //
    LEDP_16001("Problem deleting path {0} before load."), //
    LEDP_16002("Problem retrieving JSON model HDFS path for model command:{0}, yarnAppId:{1}."), //
    LEDP_16003("Problem retrieving DL sessionId for model command:{0} from url:{1}."), //
    LEDP_16004("Retrieved empty DL sessionId for model command:{0} from url:{1}."), //
    LEDP_16005("Problem retrieving DL metadata columns for model command:{0} from url:{1}."), //
    LEDP_16006("Retrieved empty DL metadata columns for model command:{0} from url:{1}."), //
    LEDP_16007("LeadScoringCommand failed."), //
    LEDP_16008("Error message received in DL metadata columns response: {0}"), //
    LEDP_16009("Problem writing metadata to HDFS path: {0}, metadata content: (1)"),//
    // Eai Service 17000-17999
    LEDP_17000("At least one attribute required."); //

    private String message;

    LedpCode(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}

package com.latticeengines.domain.exposed.exception;

//@formatter:off
public enum LedpCode {
    // Low level errors: 00000-09999
    LEDP_00000("Could not create hdfs dir {0}."), //
    LEDP_00001("Could not collect yarn queue information from ResourceManager."), //
    LEDP_00002("Generic system error."), //
    LEDP_00003("Could not find the file at path {0} on server {1}."), //

    // Validation service: 10000-10999
    LEDP_10000("Metadata schema is null."), //
    LEDP_10001("Metadata schema is not retrievable from hdfs."), //
    LEDP_10002("At least one feature required."), //
    LEDP_10003("At least one target required."), //
    LEDP_10004("Feature {0} not found in schema."), //
    LEDP_10005("Could not deserialize data schema."), //
    LEDP_10006("Name is null."), //
    LEDP_10007("Invalid name: {0}"), //

    // Metadata service: 11000-11999
    LEDP_11000("Could not load driver class {0}."), //
    LEDP_11001("Failed connecting to db."), //
    LEDP_11002("Issue running query {0}."), //
    LEDP_11003("Could not retrieve keys for table {0}."), //
    LEDP_11004("Could not retrieve metadata for table {0}."), //
    LEDP_11005("SQL column name {0} is different from the avro column name {1}."), //

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
    LEDP_12009("Failed to submit MapReduce job {0}"), //

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
    LEDP_15007("No sample avro files found in path {0}."), //
    LEDP_15014("Failed to copy metadata diagnostics file for customer: {0} from Hdfs path: {1} to: {2}."), //

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
    LEDP_16009("Problem writing data to HDFS path: {0}, data content: {1}"), //
    LEDP_16010("Problem publishing model for model command:{0}, yarnAppId:{1}."), //
    LEDP_16011("Problem publishing model-artifacts for model command:{0}, yarnAppId:{1}."), //

    // Eai Service 17000-17999
    LEDP_17000("At least one attribute required."), //
    LEDP_17001("File import can only have one data source."), //
    LEDP_17002("No metadata for attribute {0}."), //

    // PLS 18000-18999
    LEDP_18000("Problem with Global Auth URL {0}."), //
    LEDP_18001("Could not authenticate user {0}."), //
    LEDP_18002("Could not authenticate ticket {0}."), //
    LEDP_18003("Access denied."), //
    LEDP_18004("Could not register user {0}."), //
    LEDP_18005("Could not grant right {0} to user {1} for tenant {2}."), //
    LEDP_18006("Could not revoke right {0} from user {1} for tenant {2}."), //
    LEDP_18007("Model with id {0} not found."), //
    LEDP_18008("Attribute {0} cannot be null."), //
    LEDP_18009("Could not log out user ticket {0}."), //
    LEDP_18010("Could not change the password for user {0}."), //
    LEDP_18011("Could not reset the password for user {0}."), //
    LEDP_18012("Could not register tenant with id {0} and display name {1}."), //
    LEDP_18013("Could not discard tenant with id {0}."), //
    LEDP_18014("The requested new name '{0}' already exists."), //
    LEDP_18015("Could not delete user {0}."), //
    LEDP_18016("Could not get users and rights for tenant {0}."), //
    LEDP_18017("Could not get user by email {0}."), //
    LEDP_18018("Could not get user {0}."), //
    LEDP_18019("Granted right {0} from session is not a recognized privilege."), //
    LEDP_18020("Cannot parse file {0}."), //
    LEDP_18021("Cannot delete active model."), //
    LEDP_18022("Failed to download file."), //
    LEDP_18023("File not found."), //
    LEDP_18024("Cannot change deleted model to active directly; change it to inactive first, then delete it."), //
    LEDP_18025("Segment with name {0} not found."), //
    LEDP_18026("Cannot update the name of a segment. Delete and recreate."), //
    LEDP_18027("Can not provision JAMS."), //
    LEDP_18028("Provisioning PLS/GA tenant {0} through Camille failed."), //
    LEDP_18029("Can not find Org Id."), //
    LEDP_18030("Can not verify CRM credential."), //
    LEDP_18031("Can not get CRM credential."), //
    LEDP_18032("Provision VisiDB/DL Failed: {0}."), //
    LEDP_18033("Can not get topology."), //
    LEDP_18034("Can not get tenant document from Camille."), //
    LEDP_18035("Can not config system, ErrorMessage={0}."), //
    LEDP_18036("Install VisiDB Template Failed: {0}"), //
    LEDP_18037("Template {0} is not supported."), //
    LEDP_18038("Install DL Template Failed: {0}"), //
    LEDP_18039("Salesforce URL with name {0} not found."), //
    LEDP_18040("The URL field is empty of Salesforce URL with name {0}."), //
    LEDP_18041("Failed to extract information from modelSummary file."), //
    LEDP_18042("Failed to extract information from data-diagnostics file."), //
    LEDP_18043("Failed to extract information from rfModel file."), //
    LEDP_18044("Failed to extract information from top predictor file."), //
    LEDP_18045("Failed to extract information from metadata-diagnostics file."), //
    LEDP_18046("Failed to get metadata fields: {0}."), //
    LEDP_18047("Failed to update metadata field: {0}."), //
    LEDP_18048("Failed to update metadata fields: {0}."), //
    LEDP_18049("Failed to retrieve feature flags from ZK for the tenant {0}."), //

    // le-security 19000-19100
    LEDP_19000("Failed to send an email."),

    // le-admin 19101-19999
    LEDP_19101("Service service error."), //
    LEDP_19102("Service component {0} is not registered."), //
    LEDP_19103("Getting files in a server-side directory failed."), //
    LEDP_19104("The default choice [{0}] is not valid among the options {1}."), //
    LEDP_19105("The requested option list {0} does not contain the existing default choice [{1}]"), //
    LEDP_19106("Cannot define new feature flag."), //
    LEDP_19107("Cannot retrieve all the feature flag definitions."), //
    LEDP_19108("Cannot toggle the feature flag {0} for the tenant {1}."), //
    LEDP_19109("Cannot retrieve all the feature flags for the tenant {0}."), //
    LEDP_19110("Cannot remove the feature flag {0} from the tenant {1}."), //

    // le-scoring 20000-20100
    LEDP_20000("ScoringCommand Failed"), //
    LEDP_20001("Validation of the datatype failed: {0}"), //
    LEDP_20002("Python script for scoring is not provided."), //
    LEDP_20003("Lead does not have 'LeadID' column."), //
    LEDP_20004("Lead does not have 'Model_GUID' column."), //
    LEDP_20005("There are duplicate leads for 'LeadID': {0} and 'Model_GUID': {1} in one request."), //
    LEDP_20006("Datatype file for scoring is not provided."), //
    LEDP_20007("The following model(s) is(are) not provided: {0}"), //
    LEDP_20008("Cannot find any model for tenant: {0}"), //
    LEDP_20009("Not all the leads are not scored. {0} leads are transformed, but only {1} got scored."), //
    LEDP_20010("Not all the leads are transformed and stored. {0} leads are passed in, but only {1} got transformed."), //
    LEDP_20011("The scoring python script (scoring.py) failed, with the error message: {0}.}"), //
    LEDP_20012("Output file {0} does not exist.}"), //
    LEDP_20013("Scoring output file in incorrect format.}"), //
    LEDP_20014("The scoring mapper failed."), //
    LEDP_20015("The scoring mapper should not get 0 lead"), //
    LEDP_20016("The total number scoring leads is incorrect"), //
    LEDP_20017("Cannot process scoring request with 0 lead"), //

    // le-remote 21000-21999
    LEDP_21000("Problem parsing segment name or model ID from segment spec: {0}"), //
    LEDP_21001("Problem installing segment spec; DataLoader result code: {0}, error message {1}"), //
    LEDP_21002("Problem installing VisiDB structure file via DataLoader REST: {0}"), //
    LEDP_21003("Problem updating segments. Updated segment names {0} does not match existing segment names {1}"), //
    LEDP_21004("Problem installing DataLoader config file via DataLoader REST: {0}"), //
    LEDP_21005("Problem getting DataLoader tenant settings via DataLoader REST: {0}"), //
    LEDP_21006("Problem creating DataLoader tenant via DataLoader REST: {0}"), //
    LEDP_21007("Problem deleting DataLoader tenant via DataLoader REST: {0}"), //

    // le-playmaker
    LEDP_22000("Can not create data source for tenant {0}"), //
    LEDP_22001("Can not find DB connection info for tenant {0}"), //
    LEDP_22002("Tenant exists, but there's no such oauth user, tenant name={0}"), //
    LEDP_22003("Access token does not exist!"), LEDP_22004("Access token does not have token key!"), //
    LEDP_22005("Failed to get tenant!"), LEDP_22006("Failed to get tenant from DB!"), //
    LEDP_22007("Failed to get recommendations after retry."), //

    // le-upgrade
    LEDP_24000("Yarn operation exception: {0}"), //
    LEDP_24001("Jdbc operation exception: {0}"), //
    LEDP_24002("DL operation exception: {0}"), //
    LEDP_24003("PLS operation exception: {0}"), //
    LEDP_24004("ZK operation exception: {0}"), //

    // le-propdata
    LEDP_25000("Can not create new derived entitlement package {0}."),
    LEDP_25001("Derived entitlement resource error."), //
    
    // le-dataflow
    LEDP_26000("Builder bean {0} not instance of builder."), //
    LEDP_26001("Data flow context does not have values for required properties: {0}"), //
    LEDP_26002("Unknown field name {0} from previous pipe."), //
    LEDP_26003("Unknown field name {0} from previous pipe {1}."), //
    LEDP_26004("Unseen prior pipe {0}."), //
    LEDP_26005("Getting schema failed."), //
    LEDP_26006("Getting schema failed for path {0}."), //
    LEDP_26007("Table {0} has no primary key."), //
    LEDP_26008("Primary key of table {0} has no attributes."), //
    LEDP_26009("Table has no name."), //
    LEDP_26010("Extract for table {0} has no name."), //
    LEDP_26011("Extract {0} for table {1} has no path."), //
    LEDP_26012("Table {0} has no extracts.");

    private String message;

    LedpCode(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
// @formatter:on

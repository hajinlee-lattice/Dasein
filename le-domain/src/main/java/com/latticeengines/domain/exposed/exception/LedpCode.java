package com.latticeengines.domain.exposed.exception;

//@formatter:off
@SuppressWarnings("checkstyle:lineLength")
public enum LedpCode {
    // Low level errors: 00000-09999
    LEDP_00000("Could not create hdfs dir {0}."), //
    LEDP_00001("Could not collect yarn queue information from ResourceManager."), //
    LEDP_00002("Generic system error."), //
    LEDP_00003("Could not find the file at path {0} on server {1}."), //
    LEDP_00004("Could not delete hdfs dir {0}."), //
    LEDP_00005("API call rate limit reached, discarding new request: {0}"), //
    LEDP_00006("Match failed. Please go to match block log to find root cause."), //
    LEDP_00007("Generic rest call error."), //
    LEDP_00008("Match failed. Error: {0}"), //
    // Validation service: 10000-10999
    LEDP_10000("Metadata schema is null."), //
    LEDP_10001("Metadata schema is not retrievable from hdfs."), //
    LEDP_10002("At least one feature required."), //
    LEDP_10003("At least one target required."), //
    LEDP_10004("Feature {0} not found in schema."), //
    LEDP_10005("Could not deserialize data schema."), //
    LEDP_10006("Name is null."), //
    LEDP_10007("Invalid name: {0}"), //
    LEDP_10008("Unable to validate the file"), //
    LEDP_10009("Unable to find required columns {0} from the file"), //
    LEDP_10010("User type {0} is not an accepted type."), //
    LEDP_10011("File {0} cannot be found."), //
    LEDP_10012("Found unsupported character in \"{0}\" in Pivot Mapping File."), //

    // Metadata service: 11000-11999
    LEDP_11000("Could not load driver class {0}."), //
    LEDP_11001("Failed connecting to db."), //
    LEDP_11002("Issue running query {0}."), //
    LEDP_11003("Could not retrieve keys for table {0}."), //
    LEDP_11004("Could not retrieve metadata for table {0}."), //
    LEDP_11005("SQL column name {0} is different from the avro column name {1}."), //
    LEDP_11006("Tables {0} do not exist. A DataCollection must reference an existing table."), //
    LEDP_11007("Default query source already exists"), //
    LEDP_11008("Cannot find table named {0} in tenant {1}."),

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
    LEDP_12010("Failed to {0} data by using Sqoop"), //
    LEDP_12011("Failed to complete Python process due to {0}"), //
    LEDP_12012("Failed to retrieve CapacitySchedulerInfo via reflection"), //
    LEDP_12013("Failed to get prepared statement creator via reflection"), //
    LEDP_12014("Failed to execute sql query: {0}."), //

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
    LEDP_15008("Could not set input format for MapReduce."), //
    LEDP_15009("Could not set localized files for MapReduce."), //
    LEDP_15010("Could not set localized archive files for MapReduce."), //
    LEDP_15011("Could not customize sampling job for the specified sampling type."), //
    LEDP_15012("SamplingConfiguration was not set up correctly."), //
    LEDP_15013("Failed to validate SamplingConfiguration."), //
    LEDP_15014("Failed to copy metadata diagnostics file for customer: {0} from Hdfs path: {1} to: {2}."), //
    LEDP_15015("Could not set up configuration file correctly for MapReduce."), //
    LEDP_15016("Cannot model with an empty feature list."), //
    LEDP_15017("Cannot provision modeling service because {0}"), //
    LEDP_15018("Cannot invoke Data Science functionality due to {0}"), //
    LEDP_15019("Could not register Job due to error: {0}"), //

    // DLOrchestration service: 16000-16999
    LEDP_16000("Missing model command parameter(s) {0}."), //
    LEDP_16001("Problem deleting path {0} before load."), //
    LEDP_16002("Problem retrieving JSON model HDFS path for model command:{0}, yarnAppId:{1}."), //
    LEDP_16003("Problem retrieving DL sessionId for model command:{0} from url:{1}."), //
    LEDP_16004("Retrieved empty DL sessionId for model command:{0} from url:{1}."), //
    LEDP_16005("Problem retrieving DL metadata columns for model command:{0} from url:{1}."), //
    LEDP_16006("Retrieved empty DL metadata columns for model command:{0} from url:{1}."), //
    LEDP_16007("Modeling via DataLoader Orchestration failed."), //
    LEDP_16008("Error message received in DL metadata columns response: {0}"), //
    LEDP_16009("Problem writing data to HDFS path: {0}, data content: {1}"), //
    LEDP_16010("Problem publishing model for model command:{0}, yarnAppId:{1}."), //
    LEDP_16011("Problem publishing model-artifacts for model command:{0}, yarnAppId:{1}."), //

    // le-eai 17000-17999
    LEDP_17000("At least one attribute required."), //
    LEDP_17001("File import can only have one data source."), //
    LEDP_17002("No metadata for attribute {0}."), //
    LEDP_17003("Could not retrieve metadata of attributes {0} for table {1} from Endpoint."), //
    LEDP_17004("{0}'s Source Credential is Invalid. Cannot use this credential to establish connection to {1}."), //
    LEDP_17005("Failed to retrieve HttpClientConfig from Zookeeper for Tenant {0}"), //
    LEDP_17006("Got NULL Value from Last Modified Key for Tenant {0}"), //
    LEDP_17007("Got NULL Value from Table {0} for Tenant {1}"), //
    LEDP_17008("Unsupported Source Credential Check for Source Type {0}"), //
    LEDP_17009("Got NULL Value from Primary Key for Tenant {0}"), //
    LEDP_17010("Error connector configuration!"), //
    LEDP_17011("Cannot execute DL loadgroup! Exception: {0}"), //
    LEDP_17012("Cannot get loadgroup status from DL!"), //
    LEDP_17013("Cannot get table metadata from DL! Error message: {0}"), //
    LEDP_17014("Import table metadata error! {0}"), //
    LEDP_17015("Import table data error! {0}"), //
    LEDP_17016("Failed to get data from DL!"), //
    LEDP_17017("The id {0} is duplicated."), LEDP_17018("Entity {0} cannot apply merge rule {1}!"),

    // le-pls 18000-18999
    LEDP_18000("Problem with Global Auth URL {0}."), //
    LEDP_18001("The email address or password is not valid. Please re-enter your credentials."), //
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
    LEDP_18042("Failed to extract information from data-diagnostics file."), //40
    LEDP_18043("Failed to extract information from rfModel file."), //
    LEDP_18044("Failed to extract information from top predictor file."), //
    LEDP_18045("Failed to extract information from metadata-diagnostics file."), //
    LEDP_18046("Failed to get metadata fields: {0}."), //
    LEDP_18047("Failed to update metadata field: {0}."), //
    LEDP_18048("Failed to update metadata fields: {0}."), //
    LEDP_18049("Failed to retrieve feature flags from ZK for the tenant {0}."), //
    LEDP_18050("Query with name {0} not found."), //
    LEDP_18051("Column with name {0} not found."), //
    LEDP_18052("The following predictors do not exist in the predictor table: {0}."), //
    LEDP_18053("Problems uploading file {0}."), //
    LEDP_18054("Failed to start importing Salesforce data, error: {0}."), //
    LEDP_18055("Failed to start enriching data, error: {0}."), //
    LEDP_18056("Failed to start validating metadata, error: {0}."), //
    LEDP_18057("Another load group is running."), //
    LEDP_18058("Failed to get running jobs for deployment step {0}, error: {1}."), //
    LEDP_18059("Failed to get success time for deployment step {0}, error: {1}."), //
    LEDP_18060("Failed to get complete jobs for deployment step {0}, error: {1}."), //
    LEDP_18061("Failed to start running query, error: {0}."), //
    LEDP_18062("Failed to download query result data, error: {0}."), //
    LEDP_18063("Tenant deployment with tenant id {0} is not found."), //
    LEDP_18065("Problems creating payload file {0}."), //
    LEDP_18066("Problems reading payload file {0}."), //
    LEDP_18067("Could not change state of file {0} to PROCESSING."), //
    LEDP_18068("Could not copy HDFS file {0} to local."), //
    LEDP_18069("Target market with name {0} already exists."), //
    LEDP_18070("Default target market already exists."), //
    LEDP_18071("Failed to get status of load group {0}, error: {1}."), //
    LEDP_18072("Failed to run load group {0}, error: {1}."), //
    LEDP_18073("Failed to synchronize modeling and scoring, error: {0}."), //
    LEDP_18074("Tenant {0} is not found."), //
    LEDP_18075("Failed to parse attribute query {0}."), //
    LEDP_18076("Default target market already has a fit model workflow in progress."), //
    LEDP_18077("Failed to get available attributes: {0}."), //
    LEDP_18078("Failed to get saved attributes: {0}."), //
    LEDP_18079("Failed to save attributes: {0}."), //
    LEDP_18080("Failed to get template type: {0}."), //
    LEDP_18081("File {0} is already being imported."), //
    LEDP_18082("There is no target table in DataLoader."), //
    LEDP_18083("Failed to verify attributes: {0}."), //
    LEDP_18084("Could not locate file with name {0}."), //
    LEDP_18085("Failed to retrieve errors for file {0}."), //
    LEDP_18086("Failed to retrieve space configuration from ZK for the tenant {0}."), //
    LEDP_18087("Missing required fields [{0}] in csv file {1}."), //
    LEDP_18088("Could not locate table with name {0}."), //
    LEDP_18089("Failed to retrieve premium attributes limitation from ZK for the tenant {0}, error: {1}."), //
    LEDP_18090("Metadata type {0} is not supported."), //
    LEDP_18091("Artifact type {0} with name {1} already exists in module {2}."), //
    LEDP_18092("File exceeds maximum allowed limit of {0}."), //
    LEDP_18093("Failed to download errors file."), //
    LEDP_18094("Failed to read from CSV file."), //
    LEDP_18095("Detected unexpected character in header name {0}, in csv file {1}."), //
    LEDP_18096("Found empty column name in csv headers in csv file {0}."), //
    LEDP_18097("Problems uploading file."), //
    LEDP_18098("Table with name {0} not found."), //
    LEDP_18099("Could not locate table for file with name {0}."), //
    LEDP_18100("Could not locate training table for model {0}."), //
    LEDP_18101("File {0} has not been uploaded yet."), //
    LEDP_18102("Failed to download results CSV file for job {0}."), //
    LEDP_18103("Scoring job is not completed yet for job {0}."), //
    LEDP_18104("No job could be found with id {0}."), //
    LEDP_18105("Model {0} does not have attributes in the event tableName."), //
    LEDP_18106("Model {0} does not have schemaInterpretation in the modelsummary."), //
    LEDP_18107("CSV header validations failed:\n {0}"), //
    LEDP_18108("Could not find transformation group name from model {0}."), //
    LEDP_18109("Problem reading csv file header: {0}"), //
    LEDP_18110("Expected at least 1 record. Instead found 0"), //
    LEDP_18111("Copying Model {0} from tenant {1} to tenant {2} failed"), //
    LEDP_18112("Saving of lead enrichment selection failed, maximum {0} premium attributes of {1} can be selected."), //
    LEDP_18113("Saving of lead enrichment selection failed, field name {0} occurs more than once in selection."), //
    LEDP_18114("Saving of lead enrichment selection failed, field name {0} is invalid."), //
    LEDP_18115("File Validation Failed due to: {0}"), //
    LEDP_18116("Failed to Authenticate Marketo REST Credentials due to: {0}"), //
    LEDP_18117("Failed to Authenticate Marketo SOAP Credentials due to: {0}"), //
    LEDP_18118("Unable to copy source file"), //
    LEDP_18119("Cannot create marketo credential with duplicate name: {0}"), //
    LEDP_18120("Invalid file uploaded. There must be minimum of 2 columns."), //
    LEDP_18121("Model summary with id {0} must exist for campaign creation."), //
    LEDP_18122("Found reserved column name {0} in csv headers in csv file {1}."), //
    LEDP_18123("Cannot attach tenant because the authorization info is null."), //
    LEDP_18124("Model summary with id {0} has been deleted from tenant: {1}"), //
    LEDP_18125("Error retrieving pivot score chart data for model: {0}"), //
    LEDP_18126("Error retrieving bucket metadata for model: {0}"), //
    LEDP_18127("Cannot copy model with application id: {0}. Error when copying ABCD buckets"), //
    LEDP_18128("invalid_bucket_information", "The model {0} does not have valid bucket metadata information"), //

    LEDP_18129("Table name should be specified for Vdb table import workflow."), //
    LEDP_18130("Import table total rows should be greater than 0."), //
    LEDP_18131("Invalid read table data endpoint for Vdb table import workflow."), //
    LEDP_18132("Invalid Spec metadata for Vdb table import workflow."), //
    LEDP_18133("TenantId should be specified for Vdb table import workflow."), //
    LEDP_18134("Get query handle should be specified for Vdb table import workflow."), //
    LEDP_18135("Cannot recognize vdb table create rule {0}"), //
    LEDP_18136("Same load vdb table job is already submitted."), //
    LEDP_18137("Same load vdb table job is running with application id {0}"), //
    LEDP_18138("Same load vdb table job is already succeeded."), //
    LEDP_18140("Table {0} already exist, cannot create a new one"), //
    LEDP_18141("Table metadata is conflict with table already existed."), //
    LEDP_18142("Table {0} not exist, cannot import table with update rule"), //
    LEDP_18143("Invalid max or offset specified"), //
    LEDP_18144("Play name cannot be blank."), //
    LEDP_18145("Play displayName cannot be empty."), //
    LEDP_18146("Play Launch not found for {0}"), //
    LEDP_18147("User {0} do not have access for tenant {1}"), //
    LEDP_18148(
            "Standard file import only. Your file has {0} million rows. Please use drop folder for file exceeds 1 million rows."), //
    LEDP_18149("Unable to launch play with selected buckets since no Model's were selected for play {0}"), //
    LEDP_18150("Cannot get tenant configuration."), //
    LEDP_18151("Play not found for {0}."), //
    LEDP_18152("Failed to submit a customer report on matching issue."), //
    LEDP_18153("Segment not defined for Rating Engine {0}."), //
    LEDP_18154("Type not defined for Rating Engine {0}."), //
    LEDP_18155("Play {0} cannot be launched since corresponding ratings engine is not active."), //
    LEDP_18156("PlayLaunch for Play {0} cannot be started since it does not have buckets specified."), //
    LEDP_18157("PlayLaunch workflow failed."), //
    LEDP_18158("Error in evaluating restriction."), //
    LEDP_18159("Could not generate minimum required recommendations. " //
            + "Recommendations were generated for {0} accounts and for " //
            + "{1} accounts errors were encountered"), //
    LEDP_18160("Could not download result of export job {0} as it is not valid anymore."), //
    LEDP_18161("Could not download result of export job {0}."), //
    LEDP_18162("Cannot create data feed task for entity: {0}, source: {1}, feedType: {2}"), //
    LEDP_18163("Could not download result of export job {0} as it is still in progress."), //
    LEDP_18164("Could not download result of export job {0} as this export job failed."), //
    LEDP_18165("Workflow Ids cannot not be empty."), //
    LEDP_18166("Parent Job Id must be specified."), //
    LEDP_18167("SegmentExport workflow failed."), //
    LEDP_18168("Sqoop export for recommendations failed."), //
    LEDP_18169("Export could not be initiated as segment has more than {0} entries."), //
    LEDP_18170("Authentication deined as external IdentityProvider did not validate the User."), //
    LEDP_18171("Could not create valid session for External User {0}."), //
    LEDP_18172("Action Ids for job {0} is empty."), //
    LEDP_18173("SchemaInterpretation {0} not supported for cleanup file template!"), //
    LEDP_18174("Cannot transition Rating Engine: {0} from {1} to {2}"), //
    LEDP_18175("There are Plays associated with the current Rating Engine {0} and thus it cannot be deleted."), //
    LEDP_18176("PlayLaunch for Play {0} cannot be started since it does not have any launchable accounts."), //
    LEDP_18177("Websocket session {0} has NOT associated with any authentication token yet."), //
    LEDP_18178("Websocket session {0} has associated with tenant {1} and cannot associate with tenant {2}."), //
    LEDP_18179("Error finding the AI Model for {0}."), //
    LEDP_18180(
            "There is no active Rating Model associated with the current Rating Engine {0} and thus it cannot be activated."), //
    LEDP_18181("The current Rating Engine {0} is not inactive thus it cannot be deleted."), //
    LEDP_18182("Submit {0} job failed: {1}"), //
    LEDP_18183("Column name {0} has potential conflict with system defined name"), //
    LEDP_18184("Job cannot be null or incomplete"), //
    LEDP_18185("User {0} does not have the right to deactivate attribute"), //
    LEDP_18186("No scoring iteration set for Rating Engine: {0}"), //
    LEDP_18187("Cannot populate full play information since No Rating Engine associated with the Play: {0}"), //
    LEDP_18188(
            "The following field name length exceeded the limit of {0} characters. Shorten the field name to upload the file: {1}"), //
    LEDP_18189("There are Rating Models associated with the current segment {0} and thus it cannot be deleted."), //
    LEDP_18190("Update Usage Attribute Config failed due to dependency check failure"), //
    LEDP_18191("Marketo Scoring setup is missing required {0}"), //
    LEDP_18192("This Model is already configured for the given Marketo Profile {0}. Please refresh the page."), //
    LEDP_18193("Marketo Scoring Request Configuration is in invalid State. {0}."), //
    LEDP_18194("Could not find Scoring Request Configuration for given ConfigID: {0}."), //
    LEDP_18195("Update Activation Attribute Config failed due to usage enbaled"), //
    LEDP_18196("Cannot map two column to {0}, please re-map column."), //
    LEDP_18197("Could not find Marketo Credential Profile for given id: {0}."), //
    LEDP_18198("Records in company profile request cannot be empty"), //
    LEDP_18199("Could not process Scoring Request as SecretKey is not provided."), //
    LEDP_18200("Could not process Scoring Request as SecretKey doesnot match."), //
    LEDP_18201("Could not process Scoring Request as record body is empty"), //
    LEDP_18202("Could not process Scoring Request as ConfigID is empty."), //
    LEDP_18203("Validation fails and there are errors."), //
    LEDP_18204("User {0} does not have the right to update name/description of attribute"), //
    LEDP_18205("Unable to update PlayLaunch due to {0}"), //
    LEDP_18206("Play validation failure: Segment is empty"), LEDP_18207(
            "Play validation failure: Segment Name is empty"), LEDP_18208(
                    "CSV file name validations failed: CSV injection in the file name or headers"), //
    LEDP_18210("You do not have admin rights to setup automation on this tenant. Contact your Admin and setup your file transfers. For more information, visit <a href=\"http://help.lattice-engines.com/\">help.lattice-engines.com</a>"),
    LEDP_18211("Unable to cancel this action: {0}"),//
    LEDP_18212("No buckets nor launch unscored is set for play {0}, Nothing to Launch."),
    LEDP_18213("Failed while generating playlaunch export file: {0}."),
    LEDP_18214("Bulk scoring request exceeds the maximum number of rows on runtime, please try later."),
    LEDP_18215("System is under maintenance, cannot submit processAnalyze job."),
    LEDP_18216("Failed to create S3ImportSystem {0}"),
    LEDP_18217("Current CustomerSpace is NULL!"),
    LEDP_18218("Failed to get template preview {0}"),
    LEDP_18219("Unable to update PlayLaunchChannel due to {0}"),
    LEDP_18220("Play launch channel already exists for respective play {0} and system org {1}"),
    LEDP_18221("User not found"),
    LEDP_18222("Channel configuration destination given {0} doesn't match lookup id map destination {1}"),
    LEDP_18223("Failed to update import system priority: {0}"),
    LEDP_18224("AWS credentials have not been generated yet. Please ask your administrator to generate one first."),
    LEDP_18225("Play launch channel given belongs to play: {0} and does not belong play: {1}."),
    LEDP_18226("The input contains more than {0} fields. Please reduce the no. of {1} fields and try again"),
    LEDP_18227("Can't get entity data from cache when cache is in build."),
    LEDP_18228("Play Launch Channel {0} not found."),
    LEDP_18229("{0} Field Definition Request has invalid parameter: {1}"),
    LEDP_18230("Fetch Field Definition Request could not generate Fetch Response: {0}"),
    LEDP_18231("{0} Field Definition Request missing section: {1}"),

    // le-security 19000-19100
    LEDP_19000("Failed to send an email."), //
    LEDP_19001("Unauthorized."), //
    LEDP_19002("Access denied."), //
    LEDP_19003("Resource not found."), //
    LEDP_19004("External User is not allowed with Lattice Email ID."), //
    LEDP_19005("Could not validate current user as the required information is missing from Identity Provider."), //
    LEDP_19006("Sign jwt token fail."),
    LEDP_19007("No such jwt handler {0}."),
    LEDP_19008("URL encode fail."),
    LEDP_19009("No User Name key/value"),
    LEDP_19010("No User Email key/value"),
    LEDP_19011("No return_to key/value"),
    LEDP_19012("No source_ref key/value "),
    LEDP_19013("Service not found."), //
    LEDP_19014("Unsupport error."), //
    LEDP_19015("Your account has been locked, please reset password."), //
    LEDP_19016("Your login session has expired, please log in again."), //

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
    LEDP_19111("Cannot create prospecting user {0}"), //

    // le-scoring 20000-20100
    LEDP_20000("ScoringCommand Failed"), //
    LEDP_20001("User error: Validation of the datatype failed: {0}"), //
    LEDP_20002("Python script for scoring is not provided."), //
    LEDP_20003("User error: Lead does not have Unique Key Column {0}."), //
    LEDP_20004("User error: Lead does not have 'Model_GUID' column."), //
    LEDP_20005("There are duplicate leads for 'LeadID': {0} and 'Model_GUID': {1} in one request."), //
    LEDP_20007("User error: The following model is not provided: {0}"), //
    LEDP_20008("User error: Cannot find any model for tenant: {0}"), //
    LEDP_20009("Not all the leads are not scored. {0} leads are transformed, but only {1} got scored."), //
    LEDP_20010("Not all the leads are transformed and stored. {0} leads are passed in, but only {1} got transformed."), //
    LEDP_20011("The scoring python script (scoring.py) failed, with the error message: {0}.}"), //
    LEDP_20012("Output file {0} does not exist.}"), //
    LEDP_20013("Scoring output file in incorrect format.}"), //
    LEDP_20014("The scoring mapper failed."), //
    LEDP_20015("The scoring mapper should not get 0 lead"), //
    LEDP_20016("The total number scoring leads is incorrect"), //
    LEDP_20017("User error: Cannot process scoring request with 0 lead"), //
    LEDP_20018("Cannot find any lead avro files"), //
    LEDP_20019("Average avro file size cannot be 0 byte."), //
    LEDP_20020("No model has been localized for scoring."), //
    LEDP_20021("Cannot localize model {0} for tenant {1}"), //
    LEDP_20022("Invalid  Tenant name"), //
    LEDP_20023("Invalid Source data directory"), //
    LEDP_20024("Invalid  Target result directory"), //
    LEDP_20025("Invalid Unique key column"), //
    LEDP_20026("Invalid Model Guids"), //
    LEDP_20027("Maximum {0} records are allowed for bulk scoring but found {1} records"), //
    LEDP_20028("The metadata table is not configured in the RTS bulk scoring configuation: {0}"), //
    LEDP_20029("The metadata table with PID {0} does not have extracts."), //
    LEDP_20030("The extract table with PID {0} does not have valid path."), //
    LEDP_20031("The uploaded file does not have model Id column."), //
    LEDP_20032("The uploaded file does not have record Id column."), //
    LEDP_20033("This modelGuids is not set in the rts bulk scoring configuration."), //
    LEDP_20034("This row of the uploaded file had empty value for Id column with column name: {0}"), //
    LEDP_20035("This score does not have score row Id."), //
    LEDP_20036("This score does not have model Id."), //
    LEDP_20037("This score does not have valid score."), //
    LEDP_20038("This score does not have valid raw score."), //
    LEDP_20039("Lead enrichment columns {0} does not have corresponding type in the lead enrichment attribute map."), //
    LEDP_20040("Convert the sql type to avro type encounters illegal argument exception."), //
    LEDP_20041("Convert the sql type to avro type encounters illegal access exception."), //
    LEDP_20042("Unable to Initialize Score normalizer due to error: \n {0}"),

    // le-remote 21000-21999
    LEDP_21000("Problem parsing segment name or model ID from segment spec: {0}"), //
    LEDP_21001("Problem installing segment spec; DataLoader result code: {0}, error message {1}"), //
    LEDP_21002("Problem installing VisiDB structure file via DataLoader REST: {0}"), //
    LEDP_21003("Problem updating segments. Updated segment names {0} does not match existing segment names {1}"), //
    LEDP_21004("Problem installing DataLoader config file via DataLoader REST: {0}"), //
    LEDP_21005("Problem getting DataLoader tenant settings via DataLoader REST: {0}"), //
    LEDP_21006("Problem creating DataLoader tenant via DataLoader REST: {0}"), //
    LEDP_21007("Problem deleting DataLoader tenant via DataLoader REST: {0}"), //
    LEDP_21008("Problem getting spec details via DataLoader REST: {0}"), //
    LEDP_21009("Problem getting query metadata via DataLoader REST: {0}"), //
    LEDP_21010("Problem executing group via DataLoader REST; DataLoader result code: {0}, error message: {1}"), //
    LEDP_21011("Problem executing group via DataLoader REST: {0}"), //
    LEDP_21012("Problem getting launch jobs via DataLoader REST; DataLoader result code: {0}, error message: {1}"), //
    LEDP_21013("Problem getting launch jobs via DataLoader REST: {0}"), //
    LEDP_21014("Problem getting group status via DataLoader REST; DataLoader result code: {0}, error message: {1}"), //
    LEDP_21015("Problem getting group status via DataLoader REST: {0}"), //
    LEDP_21016("Problem getting group last success time: there is no success launch in DL."), //
    LEDP_21017("Problem getting group last failure launch: there is no failure launch in DL."), //
    LEDP_21018("Problem canceling launch via DataLoader REST; DataLoader result code: {0}, error message: {1}"), //
    LEDP_21019("Problem canceling launch via DataLoader REST: {0}"), //
    LEDP_21020("Problem running query via DataLoader REST; DataLoader error message: {0}"), //
    LEDP_21021("Problem running query via DataLoader REST: {0}"), //
    LEDP_21022("Problem getting query status via DataLoader REST; DataLoader error message: {0}"), //
    LEDP_21023("Problem getting query status via DataLoader REST: {0}"), //
    LEDP_21024("Problem getting query result data via DataLoader REST; DataLoader error message: {0}"), //
    LEDP_21025("Problem getting query result data via DataLoader REST: {0}"), //
    LEDP_21026("Problem getting source table metadata via DataLoader REST; DataLoader error message: {0}"), //
    LEDP_21027("Problem getting source table metadata via DataLoader REST: {0}"), //
    LEDP_21028("Invalid Marketo REST Identity endpoint: {0}"), //
    LEDP_21029("Problem communicating with Marketo REST Identity endpoint: {0}"), //
    LEDP_21030("Invalid Marketo REST clientId or clientSecret: {0} {1}"), //
    LEDP_21031("Invalid Marketo REST endpoint: {0}"), //
    LEDP_21032("Problem communicating with Marketo REST endpoint: {0}"), //
    LEDP_21033("Problem with Marketo REST endpoint: {0}"), //
    LEDP_21034("Invalid Marketo SOAP endpoint: {0}"), //
    LEDP_21035("Problem communicating with Marketo SOAP endpoint: {0}"), //
    LEDP_21036("Invalid Marketo SOAP User ID or Encryption Key: {0} {1}"), //
    LEDP_21037("Problem setting DL launch status error; DataLoader error message: {0}"), //
    LEDP_21038("Problem setting DL launch status error: {0}"), //

    // le-playmaker
    LEDP_22000("Can not create data source for tenant {0}"), //
    LEDP_22001("Can not find DB connection info for tenant {0}"), //
    LEDP_22002("Tenant exists, but there's no such oauth user, tenant name={0}"), //
    LEDP_22003("Tenant exists, but username is blank, tenant name={0}"), //
    LEDP_22004("Tenant exists, but user password is blank, tenant name={0}"), //
    LEDP_22007("Failed to get recommendations after retry."), //
    LEDP_22008("Function is not support: {0}"), //

    // le-oauth2db
    LEDP_23001("Access token does not exist!"), //
    LEDP_23002("Access token does not have token key!"), //
    LEDP_23003("Failed to get tenant!"), //
    LEDP_23004("Failed to get tenant from DB!"), //
    LEDP_23005("Failed to find Authentication for the given access token"), //
    LEDP_23006("Failed to find app_id for the given access token"), //
    LEDP_23007("Failed to find org info for the given access token"), //

    // le-upgrade
    LEDP_24000("Yarn operation exception: {0}"), //
    LEDP_24001("Jdbc operation exception: {0}"), //
    LEDP_24002("DL operation exception: {0}"), //
    LEDP_24003("PLS operation exception: {0}"), //
    LEDP_24004("ZK operation exception: {0}"), //

    // le-datacloud
    LEDP_25000("Can not create new derived entitlement package {0}."), //
    LEDP_25001("Derived entitlement resource error."), //
    LEDP_25002("Source entitlement resource error."), //
    LEDP_25003("Source column entitlement resource error."), //
    LEDP_25004("Match client {0} is not available"), //
    LEDP_25005("Column selection type {0} is not supported"), //
    LEDP_25006("Failed to get metadata for predefined column selection [{0}]"), //
    LEDP_25007("Propdata match failed. {0}"), //
    LEDP_25008("Failed to retrieve match status for root operation uid {0}"), //
    LEDP_25009("Failed to scan publication progresses"), //
    LEDP_25010("Error in handling transformation configuration"), //
    LEDP_25011("Could not start new transformation for source: {0}"), //
    LEDP_25012("Error in processing dataflow for transformation for source: {0}, Reason: {1}"), //
    LEDP_25013("Error in executing workflow for transformation for source: {0}"), //
    LEDP_25014("Could not create new transformation progress entry"), //
    LEDP_25015("Failed to find a chance to kick off a refresh of {0} after {1} retries. Errors: {2}"), //
    LEDP_25016("Invalid ingestion configuration for {0}"), //
    LEDP_25017("Failed to scan ingestion progress"), //
    LEDP_25018("Could not find schema"), //
    LEDP_25019("Could not find java type for Avro type {0}"), //
    LEDP_25020("Failed to get current version for predefined column selection [{0}]"), //
    LEDP_25021("unsupported_match_version_type", "Unsupported match version {0}"), //
    LEDP_25022("InputSourceConfig is not FileInputSourceConfig"), //
    LEDP_25023("Name and CountryCode are required in DnB realtime entity matching"), //
    LEDP_25024("Email is required in DnB realtime email matching"), //
    LEDP_25025("DnBAPIType {0} is not supported in DnB realtime matching"), //
    LEDP_25026("Updated metadata: {0} for version: {1} is not valid due to approved usage conflict"), //
    LEDP_25027("Fail to get token from DnB authentication service"), //
    LEDP_25028("Cannot find the root attribute for dimension {0}:{1}"), //
    LEDP_25029("Cannot find the attribute id for query [ {0} ]: {1}"), //
    LEDP_25030("To patch, must provide target lattice account id."), //
    LEDP_25031("The target lattice account id {0} is not valid."), //
    LEDP_25032("Must provide sufficiently many match keys."), //
    LEDP_25033("Patching domain and location (without Name) is not supported."), //
    LEDP_25034("The specified input can already match to lattice account id {0}, no need to patch."), //
    LEDP_25035("Cannot correctly parse the input domain {0}. Please remove this domain from your input."), //
    LEDP_25036("Cannot find a unique DUNS for the input. The matched DUNS is: {0}"), //
    LEDP_25037("HTTP Status: 400 Bad Request"), //
    LEDP_25038("HTTP Status: 404 Not Found"), //
    LEDP_25039("HTTP Status: 500/503 DnB System Unavailable"), //
    LEDP_25040("HTTP Status: {0} {1}"), //
    LEDP_25041("Could not get transformation progress for rootOperationId: {0}"), //
    LEDP_25042("Fail to publish entity seed & lookup entries."), //
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
    LEDP_26012("Table {0} has no extracts."), //
    LEDP_26013("Table {0} has no last modified key."), //
    LEDP_26014("Table {0} last modified key has no attributes."), //
    LEDP_26015("PivotMapper failed to convert value {0} of class {1} into class {2}."), //
    LEDP_26016("There must be at least one report aggregation specified"), //

    // le-swlib
    LEDP_27000("Software library cannot be initialized."), //
    LEDP_27001("Cannot copy local file {0} to hdfs path {1}."), //
    LEDP_27002("Software package {0} already exists."), //
    LEDP_27003("JSON file {0} not a software package."), //
    LEDP_27004("Cannot instantiate class {0} for software library initialization."), //
    LEDP_27005("Cannot execute a data flow with both path and table sources."), //

    // le-workflow
    LEDP_28000("Workflow does not exist: {0}."), //
    LEDP_28001("Problem starting workflow: {0}."), //
    LEDP_28002("Problem restarting workflow, workflowId:{0} jobexecutionId:{1}."), //
    LEDP_28003("Problem stopping workflow: {0}."), //
    LEDP_28004("Problem creating table {0}."), //
    LEDP_28005("Problem creating event table from match result; commandId:{0}."), //
    LEDP_28006("Problem sampling event table:{0}."), //
    LEDP_28007("Problem in modeling workflow for event table:{0}."), //
    LEDP_28008("Step configuration failed validation with errors:{0} ... {1}."), //
    LEDP_28009("Received Null propdata match status for this url:{0}."), //
    LEDP_28010("Modeling yarn app {0} did not succeed:{1}."), //
    LEDP_28011("Workflow name was not specified in workflow config:{0}"), //
    LEDP_28012("No models were generated from modeling steps"), //
    LEDP_28013("Not all models were downloaded. Expected model appid's:{0}, Only found model appid's:{1}"), //
    LEDP_28014("No result directory for modeling job {0}"), //
    LEDP_28015("Workflow yarn step {0} did not succeed:{1}."), //
    LEDP_28016("Could not find tenant with id:{0}"), //
    LEDP_28017("Workflow execution to be restarted does not exist:{0}"), //
    LEDP_28018("Workflow execution {0} cannot be restarted since in non-terminated state:{1}"), //
    LEDP_28019("Problem modeling for PMML:{0}."), //
    LEDP_28020("Module {0} must have a PMML file."), //
    LEDP_28021("Could not get customerspace from workflow configuration."), //
    LEDP_28022("Could not get workflow job from null applicationId."), //
    LEDP_28023("Could not find workflow job by applicationId {0}."), //
    LEDP_28024("Received Null propdata match status for this root operation uid: {0}."), //
    LEDP_28025("PMML file {0} not found for module {1}."), //
    LEDP_28026("Pivot file {0} not found for module {1}."), //
    LEDP_28027("Problem reading data rules from {0}."), //
    LEDP_28028("Unsupported PMML version {0} detected. We only support {1}."), //
    LEDP_28029("Download model summary failed."), //
    LEDP_28030("Problem in event counting: {0}."), //
    LEDP_28031("Workflow yarn step {0} has lost node error: {1}."), //

    // le-dellebi
    LEDP_29000(
            "The file type {0} is not found in the DellEBI configuration database. Please check the config database."), //
    LEDP_29001("Must define one type to retrieve."), //
    LEDP_29002("The input parameter {0} is null."), //
    LEDP_29003("The file {0} is not recorded in the Execution_Log table."), //
    LEDP_29004("Failed to download or unzip File , name={0}."), //
    LEDP_29005("The bean {0} is not found in the DellEBI configuration database. Please check the config database."), //
    LEDP_29006(
            "The file name {0} did not match the file pattern in the DellEBI configuration database. Please check the config database."), //
    LEDP_29007("Cannot get the bean name. Did you change the quartz bean name for Dell ebi"), //

    // le-quartz 30000-31000
    LEDP_30000("Destination Url {0} invalid."), //
    LEDP_30001("The required job bean {0} does not exist."), //
    LEDP_30002("Can't get host address."), //

    // le-scoringapi internal facing errors
    LEDP_31000("Failed to retrieve file from HDFS {0}"), //
    LEDP_31001("Failed to get model json hdfs path from HDFS {0}"), //
    LEDP_31002("Failed to copy model json from hdfs {0} to local {1}"), //
    LEDP_31003("Model file does not exist {0}"), //
    LEDP_31004("Too many model files exist at {0}"), //
    LEDP_31005("Matched fieldname size {0} does not equal matched fieldvalue size {1}"), //
    LEDP_31006("Problem creating model artifacts directory {0}"), //
    LEDP_31007("ModelSummary {0} is missing appId value and there are more than 1 potential appId subfolders {1}"), //
    LEDP_31008("ModelSummary {0} is missing eventTableName value"), //
    LEDP_31009("Retrieved null EventTable {0}"), //
    LEDP_31010("EventTable {0} interpretation is null or empty"), //
    LEDP_31011("No percentile buckets found"), //
    LEDP_31012("PMML model has multiple ({0}) results and no target was specified"), //
    LEDP_31013("PMML model evaluation returned no results"), //
    LEDP_31014("Problem scoring the record {0}"), //
    LEDP_31015("Failed to get data export csv hdfs path from HDFS {0}"), //
    LEDP_31016("Data export csv does not exist {0}"), //
    LEDP_31017("Too many Data export csv files exist at {0}"), //
    LEDP_31018("Failed to get hdfs path from HDFS {0}"), //
    LEDP_31019("Scored txt does not exist {0}"), //
    LEDP_31020("Too many scored txt files exist at {0}"), //
    LEDP_31021("Could not find ID field name from datacomposition schema {0}"), //
    LEDP_31022("No match found"), //
    LEDP_31023("Get enrichment for one record failed."), //
    LEDP_31024("Score a single record failed."), //
    LEDP_31025("Score a bulk record failed."), //
    LEDP_31026("Pmml file for model {0} too big to load into cache"), //
    LEDP_31027("Fail to retrieve ModelSummary {0} from pls"), //

    // le-scoringapi external-facing errors
    LEDP_31101("missing_model_id", "modelId is required"), //
    LEDP_31102("invalid_model_id", "{0} is not a valid activated model"), //
    LEDP_31103("problem_populating_missing_field", "Problem populating missing field {0} with value {1}"), //
    LEDP_31104("problem_scoring_missing_fields", "Problem scoring the record due to missing fields {0}"), //
    LEDP_31105("mismatched_datatype", "Input record contains columns that do not match expected datatypes: {0}"), //
    LEDP_31106("invalid_start_date", "{0} is not a valid start date"), //
    LEDP_31107("unsupported_model_type", "Unsupported model type {0}"), //
    LEDP_31108("error_scoring_data", "Error in calculating score for input record, Cause: {0}"), //
    LEDP_31109("error_parsing_data", "Input record contains columns that are not parsable. Cause: {0}"), //
    LEDP_31110("error_transforming_data", "Input record contains columns that cannot be transformed. Cause: {0}"), //
    LEDP_31111("api_error", "Could not process record. Cause: {0}"), //
    LEDP_31112("enrichment_config_error", "Error while extracting enrichment configuration. Cause: {0}"), //
    LEDP_31113("missing_domain", "Either email or website is required"), //
    LEDP_31114("inactive_model", "The model {0} is not active for Real time scoring"), //
    LEDP_31115("check_system_error", "Error while check system status. Cause: {0}"), //
    LEDP_31199("missing_domain",
            "Required field(s) are missing: {0}. In case of lead type model, " //
                    + "make sure to specify 'Email' field and for non-lead type model " //
                    + "specify either 'Website' or 'Domain' fields. If these fields " //
                    + "are not specified then specify either 'CompanyName' " //
                    + "or 'DUNS' field."), //
    LEDP_31200("invalid_bucket_information", "The model {0} does not have valid bucket metadata information"), //

    // Generic Errors
    LEDP_32000("Validations failed: {0}"), //
    LEDP_32001("Unsupported Operation: {0}"), //

    // le-saml
    LEDP_33000("An identity provider with ID {0} already exists"), //
    LEDP_33001("Validation failed for Identity Provider configuration: {0}"), //
    LEDP_33002("Tenant is already has SSO/SAML configuration. Idp entity id: {0}"), //
    LEDP_33003("Cannot find Tenant: {0}"), //
    LEDP_33004("Could not initiate the login for Tenant: {0}"), //
    LEDP_33005("Tenant Name cannot be empty"), //
    LEDP_33006("Could not initiate the login for Tenant: {0}, as its SSO url is missing"), //

    // le-encryption
    LEDP_34000("Directory {0} already exists and is non-empty.  Encryption requires a completely new customer with "
            + "empty or non-existent data directories.  Try a different customer id"), //
    LEDP_34001("Could not create key for customer {0}"), //
    LEDP_34002("Could not delete key for customer {0}"), //
    LEDP_34003("Could not locate tenant with name {0}"), //

    // le-modelquality
    LEDP_35000("No {0} with name {1} found"), // No {Entity} with name {Name}
                                              // found
    LEDP_35001("Failed to save PipelineStep {0}"), //
    LEDP_35002("{0} with name {1} already exists"), //
    LEDP_35003("{0} cannot be empty"), //
    LEDP_35004("{0} cannot be empty for {1} type tenant"), //
    LEDP_35005("Training set not found in HDFS for Tenant: {0} and modelID: {1}"), //
    LEDP_35006("No SchemaInterpretation found for the training set specified for Tenant: {0} and modelID: {1}"), //
    LEDP_35007("The pipeline.json file was not found in the location : {0}"), //

    // le-app
    LEDP_36001("Invalid attribute name: {0}"), //
    LEDP_36002(
            "Failed to execute a query. Please make sure your data collection has been properly initialized, or contact our service for further assist."), //

    // le-query
    LEDP_37000("Lookup must have range specified"), //
    LEDP_37001("Could not locate bucket to resolve bucketed attribute {0} for specified value {1}."), //
    LEDP_37002("Must implement BusinessObject.processFreeFormSearch for object {0}"), //
    LEDP_37003("Could not find table of type {0} in data collection"), //
    LEDP_37004("Cannot find 1-to-1 or many-to-1 relationship to satisfy necessary join {0}"), //
    LEDP_37005("Cannot find 1-to-many relationship to satisfy necessary join {0}"), //
    LEDP_37006("Unsupported relation {0}"), //
    LEDP_37007("Could not locate BusinessObject for ObjectType {0}"), //
    LEDP_37008(
            "Could not find a one-to-many relationship from table {0} to table of type {1} to process exists restriction {2}"), //
    LEDP_37009("Unsupported restriction {0}"), //
    LEDP_37010("Could not find attribute with name {0} in table {1}"), //
    LEDP_37011("Unsupported lookup type {0}"), //
    LEDP_37012("Failed to retrieve data from {0}"), //
    LEDP_37013("Could not find data collection of type {0}"), //
    LEDP_37014("Could not find the default data collection"), //
    LEDP_37015(
            "Could not find the attribute repository for tenant {0} at version {1}. Make sure data profiling is done for the tenant."), //
    LEDP_37016("Cannot support cross-period event query, found {0} and {1} in the query."), //
    LEDP_37017("Unable to find the {0} table for BusinessEntity {1} for Customer space {2}"), //

    // le-dante
    LEDP_38001("Could not find Talking point with name {0}"), //
    LEDP_38002("Failed to create/update Talking points"), //
    LEDP_38003("Could not find accounts for Tenant {0}"), //
    LEDP_38004("Cannot return less than 1 account"), //
    LEDP_38005("Failed to find the MetadataDocument for the CustomerSpace {0}"), //
    LEDP_38006("Failed to parse account attributes from MetadataDocument"), //
    LEDP_38007("Failed to populate account attributes"), //
    LEDP_38008("No tenant found for the request"), //
    LEDP_38009("Invalid Customer space {0}"), //
    LEDP_38010("Unable to compile resources for the talking point preview"), //
    LEDP_38011("PlayName should be same for all talking points"), //
    LEDP_38012("Failed to find the play by name {0}"), //
    LEDP_38013("Failed to retrieve the play"), //
    LEDP_38014("Failed to publish Talking Points for the play {0}, CustomerSpace {1}"), //
    LEDP_38015("Failed to populate TalkingPoint preview for the play {0}, CustomerSpace {1}"), //
    LEDP_38016("Failed to retrieve Talking Points for the play {0}"), //
    LEDP_38017("Unable to delete Talking points {0} due to an internal error"), //
    LEDP_38018("Talking point not associated with any Play"), //
    LEDP_38019("Failed to revert Talking points for the play {0}, CustomerSpace {1}"), //
    LEDP_38020("Unable to create lead in Dante for the recommendation {0}, play {1} ,CustomerSpace {2}"), //
    LEDP_38021("Empty or null recommendation, Failed to create lead in dante for CustomerSpace {0}"), //
    LEDP_38022("Empty or null play, Failed to create lead in dante for CustomerSpace {0}"), //
    LEDP_38023("No account attributes found for tenant {0}"), //

    // le-ulysses
    LEDP_39001("No account with id {0} found for CustomerSpace {1}"), //
    LEDP_39002("Could not find Tenant information"), //
    LEDP_39003("Expected only one record of the {0} entity but found {1}"), //
    LEDP_39004("No AccountId attribute found"), //
    LEDP_39005("Failed to compile the MetadataDocument for the CustomerSpace {0}"), //
    LEDP_39008("No SpendAnalyticsSegment data found"), //

    // le-cdl
    LEDP_40000("CustomerSpace in CleanupByDateRangeConfig is null."), //
    LEDP_40001("Entity in CleanupByDateRangeConfig is not Transaction."), //
    LEDP_40002("StartTime or EndTime in CleanupByDateRangeConfig is null."), //
    LEDP_40003("StartTime cannot after EndTime in CleanupByDateRangeConfig."), //
    LEDP_40004("Date to day period error. Error message: {0}."), //
    LEDP_40005("Table is null. CustomerSpace {0}."), //
    LEDP_40006("There are 0 or more than 1 extract in table. CustomerSpace {0}."), //
    LEDP_40007("Cleanup operation {0} should have attribute {1} in template!"), //
    LEDP_40008("Invoke time should between 0 and 23."), //
    LEDP_40009(
            "RatingEngine {0} or RatingModel {1} type unsupported for modeling query generation. Customer Space {2}"), //
    LEDP_40010("Invalid ModelingQueryType {0}."), //
    LEDP_40011("Cross sell Repeat Purchase model {0} is missing PURCHASED_BEFORE_PERIOD configuration."), //
    LEDP_40012("Cannot model Cross-Sell AI Model {0} since no target products were defined. Customer Space {1}"), //
    LEDP_40013("No Rating Engine provided. Customer Space {0}"), //
    LEDP_40014("RatingModel for the Rating Engine {0} cannot be null. Customer Space {1}"), //
    LEDP_40015("Cannot parse {0} to a valid starting date"), //
    LEDP_40016("No Rating Engine with ID {0} found for Customer Space {1}"), //
    LEDP_40017("Unsupported Modeling strategy"), //
    LEDP_40018("Unable to submit modeling job for RatingEngine {0}, No target segment set"), //
    LEDP_40019("Unsupported Custom Event Modeling Type {0}"), //
    LEDP_40020("Unable to find a Model Summary for the AIModel {0} of the RatingEngine {1}, Customer Space {2}"), //
    LEDP_40021("Unable to set the default model name correctly, setting it to {0}"), //
    LEDP_40022("Serving store is empty or wrong customer config leads to empty store"), //
    LEDP_40023("Wrong customer config, system can't render these attributes {0} "), //
    LEDP_40024("Dependency detection among rating engine. {0}"), //
    LEDP_40025("Dependency detection among segment. {0}"), //
    LEDP_40026("invalid attribute config {0} after render"), //
    LEDP_40027("invalid attribute config {0} in DB"), //
    LEDP_40028("Cannot change job Status for ai model {0} since the current status is already terminal"), //
    LEDP_40029("RatingModel {0} not associated with Rating Engine {1}"), //
    LEDP_40030("BucketMetadata not provided for Iteration {0} while setting ScoringIteration for Model {1}"), //
    LEDP_40031("No ModelSummary for Iteration {0}, cannot set it as the ScoringIteration for Model {1}"), //
    LEDP_40032("Curated attribute validation failed due to: {0}"), //
    LEDP_40033("Cannot model iteration {0} of the Cross-Sell Model {1} since number of positive events({2}) is less " + //
            "then minimum required({3}). Customer Space {4}"), //
    LEDP_40034("Modeling not complete yet for Iteration {0} of Model {1}. Customer Space {2}"), //
    LEDP_40035("Modeling did not succeed for Iteration {0} of Model {1}. Customer Space {2}"), //
    LEDP_40036("Unable to find {0} for Iteration {1} of Model {2}. Customer Space {3}"), //
    LEDP_40037("Feature Importance was not populated in file {0} for ModelSUmmary {1}. Customer Space {2}"), //
    LEDP_40038("Iterations are not supported for {0} Models. Customer Space {1}"), //
    LEDP_40039("Cannot create iteration since no iteration to derive from has been provided. Customer Space {0}"), //
    LEDP_40040("Cannot create an iteration derived from iteration of another Model. Customer Space {0}"), //
    LEDP_40041("Cannot perform action as potential circular dependencies found: {0}"), //
    LEDP_40042("Cannot perform deletion as there are direct dependencies: {0}"), //
    LEDP_40043("S3 File missing the following required fields: {0}"), //
    LEDP_40044("Please add Attributes to your model. You can also change usage settings in Attribute Management"), //
    LEDP_40045("Cannot count rating models coverage for segment {0}, as it doesn't exists"),
    LEDP_40046("Too few events to learn from - the model needs at least {0} successes. Try modeling on "
            + "more products or adding more accounts to your Segment. You can also use My Data to learn more about your selling history."),
    LEDP_40047("Invalid state transition from {0} to {1}"), //
    LEDP_40048("Workflow status handler does not exist for event type {0}"),
    LEDP_40049("Cannot create External System Authentication as LookupMap is empty"),
    LEDP_40050("Cannot create External System Authentication as LookupMap with Id {0} doesnot exists"),
    LEDP_40051("Cannot retrieve External System Authentication as AuthID is empty"),
    LEDP_40052("Could not find External System Authentication ID: {0}"),
    LEDP_40053("Unable to parse ModelSummary payload for Iteration {0} of Model {1}"),
    LEDP_40054("We are currently experiencing heavy data loads. Please try again later."),
    LEDP_40055("CSV file has duplicate headers! (case insensitive)"),
    LEDP_40056("Failed to submit S3 import job, error: {0}"),
    LEDP_40057("Detected invalid buckets: {0}"),
    LEDP_40058("Cannot create an iteration derived from an incomplete model. Customer Space: {0}"),
    LEDP_40059("{0} error when validating with input file, please reference {1} for details."),
    LEDP_40060("Invalid business calendar"),
    LEDP_40061("Cannot update Import System: {0}"),
    LEDP_40062("Cannot update All System Priority cause there's {0} systems and {1} systems in update request."),
    LEDP_40063("Cannot update All System Priority, missing system: {0}."),
    LEDP_40064("Cannot update All System Priority: {0}"),
    LEDP_40065("Cannot create a new Campaign without a Display Name"),
    LEDP_40066("Cannot create Import System: {0}"),
    LEDP_40067("Cannot update Export Field Metadata Mapping as LookupMap is empty"),
    LEDP_40068("Cannot find Export Field Metadata Service for CDL External System Name {0}."),
    LEDP_40069("Cannot find attribute name {0} in Export Field Metadata Defaults for {1} or in serving store."),

    // le-domain
    LEDP_41001("PeriodName field {0} is null for record {1}."),
    LEDP_41002("PeriodId field {0} is null for record {1}."),
    LEDP_41003("Fail to shutdown batch write executor properly"),

    // le-db
    LEDP_50000("Object {0} is not soft deletable."); //


    private String message;

    private String externalCode;

    LedpCode(String message) {
        this.message = message;
    }

    LedpCode(String externalCode, String message) {
        this.externalCode = externalCode;
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public String getExternalCode() {
        return externalCode;
    }

}
// @formatter:on

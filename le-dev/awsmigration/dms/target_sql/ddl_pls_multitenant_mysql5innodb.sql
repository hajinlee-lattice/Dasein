use PLS_MultiTenant;
drop table if exists `ALGORITHM`;
drop table if exists `ATTRIBUTE_CUSTOMIZATION_PROPERTY`;
drop table if exists `AccountMaster_Accounts`;
drop table if exists `AccountMaster_Attributes`;
drop table if exists `AccountMaster_Contacts`;
drop table if exists `BUCKET_METADATA`;
drop table if exists `CATEGORY_CUSTOMIZATION_PROPERTY`;
drop table if exists `DATAFLOW_JOB`;
drop table if exists `DATAPLATFORM_YARNMETRIC_GENERATORINFO`;
drop table if exists `EAI_JOB`;
drop table if exists `ENRICHMENT`;
drop table if exists `JOB`;
drop table if exists `KEY_VALUE`;
drop table if exists `MARKETO_CREDENTIAL`;
drop table if exists `MARKETO_MATCH_FIELD`;
drop table if exists `METADATA_ARTIFACT`;
drop table if exists `METADATA_ATTRIBUTE`;
drop table if exists `METADATA_DATA_COLLECTION_PROPERTY`;
drop table if exists `METADATA_DATA_COLLECTION`;
drop table if exists `METADATA_EXTRACT`;
drop table if exists `METADATA_HIERARCHY`;
drop table if exists `METADATA_LASTMODIFIED_KEY`;
drop table if exists `METADATA_LEVEL`;
drop table if exists `METADATA_MODULE`;
drop table if exists `METADATA_PRIMARY_KEY`;
drop table if exists `METADATA_SEGMENT_PROPERTY`;
drop table if exists `METADATA_SEGMENT`;
drop table if exists `METADATA_STORAGE`;
drop table if exists `METADATA_TABLE_RELATIONSHIP`;
drop table if exists `METADATA_TABLE_TAG`;
drop table if exists `METADATA_TABLE`;
drop table if exists `METADATA_VDB_EXTRACT`;
drop table if exists `MODELING_JOB`;
drop table if exists `MODELQUALITY_ALGORITHM_PROPERTY_DEF`;
drop table if exists `MODELQUALITY_ALGORITHM_PROPERTY_VALUE`;
drop table if exists `MODELQUALITY_ALGORITHM`;
drop table if exists `MODELQUALITY_ANALYTIC_PIPELINE`;
drop table if exists `MODELQUALITY_ANALYTIC_TEST`;
drop table if exists `MODELQUALITY_AP_TEST_AP_PIPELINE`;
drop table if exists `MODELQUALITY_AP_TEST_DATASET`;
drop table if exists `MODELQUALITY_DATAFLOW`;
drop table if exists `MODELQUALITY_DATASET`;
drop table if exists `MODELQUALITY_MODELCONFIG`;
drop table if exists `MODELQUALITY_MODELRUN`;
drop table if exists `MODELQUALITY_PIPELINE_PIPELINE_STEP`;
drop table if exists `MODELQUALITY_PIPELINE_PROPERTY_DEF`;
drop table if exists `MODELQUALITY_PIPELINE_PROPERTY_VALUE`;
drop table if exists `MODELQUALITY_PIPELINE_STEP`;
drop table if exists `MODELQUALITY_PIPELINE`;
drop table if exists `MODELQUALITY_PROPDATA`;
drop table if exists `MODELQUALITY_SAMPLING_PROPERTY_DEF`;
drop table if exists `MODELQUALITY_SAMPLING_PROPERTY_VALUE`;
drop table if exists `MODELQUALITY_SAMPLING`;
drop table if exists `MODELQUALITY_SCORING_DATASET`;
drop table if exists `MODELREVIEW_COLUMNRESULT`;
drop table if exists `MODELREVIEW_DATARULE`;
drop table if exists `MODELREVIEW_ROWRESULT`;
drop table if exists `MODEL_DEFINITION`;
drop table if exists `MODEL_SUMMARY_DOWNLOAD_FLAGS`;
drop table if exists `MODEL_SUMMARY_PROVENANCE_PROPERTY`;
drop table if exists `MODEL_SUMMARY`;
drop table if exists `MODEL`;
drop table if exists `OAUTH2_ACCESS_TOKEN`;
drop table if exists `PREDICTOR_ELEMENT`;
drop table if exists `PREDICTOR`;
drop table if exists `PROSPECT_DISCOVERY_OPTION`;
drop table if exists `QUOTA`;
drop table if exists `REPORT`;
drop table if exists `SALESFORCE_URL`;
drop table if exists `SEGMENT`;
drop table if exists `SELECTED_ATTRIBUTE`;
drop table if exists `SOURCE_FILE`;
drop table if exists `TARGET_MARKET_DATA_FLOW_OPTION`;
drop table if exists `TARGET_MARKET_REPORT_MAP`;
drop table if exists `TARGET_MARKET`;
drop table if exists `TENANT_DEPLOYMENT`;
drop table if exists `TENANT`;
drop table if exists `THROTTLE_CONFIGURATION`;
drop table if exists `WORKFLOW_JOB`;
create table `ALGORITHM` (`ALGO_TYPE` varchar(31) not null, `PID` bigint not null auto_increment unique, `ALGORITHM_PROPERTIES` longtext, `CONTAINER_PROPERTIES` longtext, `NAME` varchar(255) not null, `PIPELINE_DRIVER` varchar(255), `PIPELINE_LIB_SCRIPT` varchar(255), `PIPELINE_PROPERTIES` longtext, `PIPELINE_SCRIPT` varchar(255), `PRIORITY` integer not null, `SAMPLE_NAME` varchar(255), `SCRIPT` varchar(500), FK_MODEL_DEF_ID bigint, primary key (`PID`)) ENGINE=InnoDB;
create table `ATTRIBUTE_CUSTOMIZATION_PROPERTY` (`PID` bigint not null auto_increment unique, `PROPERTY_NAME` varchar(255) not null, `PROPERTY_VALUE` varchar(255), `ATTRIBUTE_NAME` varchar(100) not null, `CATEGORY_NAME` varchar(255) not null, `TENANT_PID` bigint not null, `USE_CASE` varchar(255) not null, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`TENANT_PID`, `ATTRIBUTE_NAME`, `USE_CASE`, `CATEGORY_NAME`, `PROPERTY_NAME`)) ENGINE=InnoDB;
create table `AccountMaster_Accounts` (`LatticeAccountID` bigint not null unique, `City` varchar(255), `Country` varchar(255), `Domain` varchar(255), `EmployeesRange` varchar(255), `Industry` varchar(255), `InsideViewID` integer, `Name` varchar(255), `ParentID` bigint, `Region` varchar(255), `RevenueRange` varchar(255), `State` varchar(255), `Street` varchar(255), `SubIndustry` varchar(255), primary key (`LatticeAccountID`)) ENGINE=InnoDB;
create table `AccountMaster_Attributes` (`PID` bigint not null auto_increment unique, `AttrKey` varchar(255) not null, `AttrValue` varchar(255) not null, `ParentKey` varchar(255), `ParentValue` varchar(255), `Source` varchar(255), primary key (`PID`)) ENGINE=InnoDB;
create table `AccountMaster_Contacts` (`PID` bigint not null unique, `ContactID` varchar(255) not null unique, `CompanyID` bigint, `Email` varchar(255), `FirstName` varchar(255), `JobLevel` varchar(255), `JobType` varchar(255), `LastName` varchar(255), `Phone` varchar(255), `Titles` varchar(255), primary key (`PID`, `ContactID`)) ENGINE=InnoDB;
create table `BUCKET_METADATA` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null, `CREATION_TIMESTAMP` bigint not null, `LAST_MODIFIED_BY_USER` varchar(255), `LEFT_BOUND_SCORE` integer not null, `LIFT` double precision not null, `NUM_LEADS` integer not null, `RIGHT_BOUND_SCORE` integer not null, MODEL_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `CATEGORY_CUSTOMIZATION_PROPERTY` (`PID` bigint not null auto_increment unique, `PROPERTY_NAME` varchar(255) not null, `PROPERTY_VALUE` varchar(255), `CATEGORY_NAME` varchar(255) not null, `TENANT_PID` bigint not null, `USE_CASE` varchar(255) not null, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`TENANT_PID`, `USE_CASE`, `CATEGORY_NAME`, `PROPERTY_NAME`)) ENGINE=InnoDB;
create table `DATAFLOW_JOB` (`CUSTOMER` varchar(255), `DATAFLOW_BEAN_NAME` varchar(255), `JOB_PID` bigint not null, primary key (`JOB_PID`)) ENGINE=InnoDB;
create table `DATAPLATFORM_YARNMETRIC_GENERATORINFO` (`PID` bigint not null auto_increment unique, `FINISHED_TIMEBEGIN` bigint not null, `NAME` varchar(255) not null unique, primary key (`PID`)) ENGINE=InnoDB;
create table `EAI_JOB` (`TABLES` varchar(255), `JOB_PID` bigint not null, primary key (`JOB_PID`)) ENGINE=InnoDB;
create table `ENRICHMENT` (`PID` bigint not null auto_increment unique, `TENANT_ID` bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `JOB` (`JOB_PID` bigint not null auto_increment unique, `APPMASTER_PROPERTIES` longtext, `CLIENT` varchar(255) not null, `CONTAINER_PROPERTIES` longtext, `CUSTOMER` varchar(255), `JOB_ID` varchar(255) not null, primary key (`JOB_PID`)) ENGINE=InnoDB;
create table `KEY_VALUE` (`PID` bigint not null auto_increment unique, `DATA` longblob not null, `OWNER_TYPE` varchar(255) not null, `TENANT_ID` bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `MARKETO_CREDENTIAL` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null, `TENANT_ID` bigint not null, `REST_CLIENT_ID` varchar(255) not null, `REST_CLIENT_SECRET` varchar(255) not null, `REST_ENDPOINT` varchar(255) not null, `REST_IDENTITY_ENDPOINT` varchar(255) not null, `SOAP_ENCRYTION_KEY` varchar(255) not null, `SOAP_ENDPOINT` varchar(255) not null, `SOAP_USER_ID` varchar(255) not null, FK_TENANT_ID bigint not null, FK_ENRICHMENT_ID bigint not null, primary key (`PID`), unique (`NAME`, `TENANT_ID`)) ENGINE=InnoDB;
create table `MARKETO_MATCH_FIELD` (`PID` bigint not null auto_increment unique, `MARKETO_FIELD_NAME` varchar(255), `MARKETO_MATCH_FIELD_NAME` varchar(255) not null, `TENANT_ID` bigint not null, ENRICHMENT_ID bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `METADATA_ARTIFACT` (`PID` bigint not null auto_increment unique, `TYPE` integer not null, `NAME` varchar(255) not null, `PATH` varchar(766) not null, `TENANT_ID` bigint not null, FK_MODULE_ID bigint not null, primary key (`PID`), unique (`TENANT_ID`, `PATH`)) ENGINE=InnoDB;
create table `METADATA_ATTRIBUTE` (`PID` bigint not null auto_increment unique, `ENUM_VALUES` varchar(2048), `DISPLAY_NAME` varchar(255) not null, `LENGTH` integer, `LOGICAL_DATA_TYPE` varchar(255), `NAME` varchar(255) not null, `NULLABLE` boolean not null, `DATA_TYPE` varchar(255) not null, `PRECISION` integer, `PROPERTIES` longblob not null, `SCALE` integer, `SOURCE_LOGICAL_DATA_TYPE` varchar(255), `TENANT_ID` bigint not null, `VALIDATORS` longblob, FK_TABLE_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `METADATA_DATA_COLLECTION_PROPERTY` (`PID` bigint not null auto_increment unique, `PROPERTY` varchar(255) not null, `VALUE` varchar(2048), FK_DATA_COLLECTION_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `METADATA_DATA_COLLECTION` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null, `TENANT_ID` bigint not null, `TYPE` varchar(255) not null, FK_TENANT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `METADATA_EXTRACT` (`PID` bigint not null auto_increment unique, `EXTRACTION_TS` bigint not null, `NAME` varchar(255) not null, `PATH` varchar(2048) not null, `PROCESSED_RECORDS` bigint not null, `TENANT_ID` bigint not null, FK_TABLE_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `METADATA_HIERARCHY` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null, FK_TABLE_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `METADATA_LASTMODIFIED_KEY` (`PID` bigint not null auto_increment unique, `ATTRIBUTES` varchar(2048) not null, `DISPLAY_NAME` varchar(255) not null, `NAME` varchar(255) not null, `LAST_MODIFIED_TS` bigint not null, FK_TABLE_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `METADATA_LEVEL` (`PID` bigint not null auto_increment unique, `ATTRIBUTES` varchar(2048) not null, `DISPLAY_NAME` varchar(255) not null, `NAME` varchar(255) not null, `ORDER_IN_HIERARCHY` integer not null, FK_HIERARCHY_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `METADATA_MODULE` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null, `TENANT_ID` bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`TENANT_ID`, `NAME`)) ENGINE=InnoDB;
create table `METADATA_PRIMARY_KEY` (`PID` bigint not null auto_increment unique, `ATTRIBUTES` varchar(2048) not null, `DISPLAY_NAME` varchar(255) not null, `NAME` varchar(255) not null, FK_TABLE_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `METADATA_SEGMENT_PROPERTY` (`PID` bigint not null auto_increment unique, `PROPERTY` varchar(255) not null, `VALUE` varchar(2048), METADATA_SEGMENT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `METADATA_SEGMENT` (`PID` bigint not null auto_increment unique, `CREATED` datetime not null, `DESCRIPTION` varchar(255), `DISPLAY_NAME` varchar(255) not null, `NAME` varchar(255) not null, `RESTRICTION` longtext, `TENANT_ID` bigint not null, `UPDATED` datetime not null, FK_DATA_COLLECTION_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `METADATA_STORAGE` (`NAME` varchar(31) not null, `PID` bigint not null auto_increment unique, `TABLE_NAME` varchar(255) not null, `DATABASE_NAME` integer, FK_TABLE_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `METADATA_TABLE_RELATIONSHIP` (`PID` bigint not null auto_increment unique, `SOURCE_ATTRIBUTES` longtext, `SOURCE_CARDINALITY` integer not null, `TARGET_ATTRIBUTES` longtext, `TARGET_CARDINALITY` integer not null, `TARGET_TABLE_NAME` varchar(255) not null, FK_SOURCE_TABLE_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `METADATA_TABLE_TAG` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null, `TENANT_ID` bigint not null, FK_TABLE_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `METADATA_TABLE` (`PID` bigint not null auto_increment unique, `DISPLAY_NAME` varchar(255) not null, `INTERPRETATION` varchar(255), `MARKED_FOR_PURGE` boolean not null, `NAME` varchar(255) not null, `NAMESPACE` varchar(255), `TYPE` integer not null, `TENANT_ID` bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`TENANT_ID`, `NAME`, `TYPE`)) ENGINE=InnoDB;
create table `METADATA_VDB_EXTRACT` (`PID` bigint not null auto_increment unique, `EXTRACT_IDENTIFIER` varchar(255) not null unique, `EXTRACTION_TS` datetime not null, `LINES_PER_FILE` integer, `LOAD_APPLICATION_ID` varchar(255), `PROCESSED_RECORDS` integer not null, `IMPORT_STATUS` varchar(255) not null, `TARGET_PATH` varchar(2048), primary key (`PID`)) ENGINE=InnoDB;
create table `MODELING_JOB` (`CHILD_JOB_IDS` varchar(255), `PARENT_PID` bigint, `JOB_PID` bigint not null, FK_MODEL_ID bigint, primary key (`JOB_PID`)) ENGINE=InnoDB;
create table `MODELQUALITY_ALGORITHM_PROPERTY_DEF` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null, FK_ALGORITHM_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `MODELQUALITY_ALGORITHM_PROPERTY_VALUE` (`PID` bigint not null auto_increment unique, `VALUE` varchar(255), FK_ALGORITHM_PROPDEF_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `MODELQUALITY_ALGORITHM` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null unique, `SCRIPT` varchar(255) not null, `TYPE` integer, `VERSION` integer, primary key (`PID`), unique (`NAME`)) ENGINE=InnoDB;
create table `MODELQUALITY_ANALYTIC_PIPELINE` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null unique, `VERSION` integer, FK_ALGORITHM_ID bigint not null, FK_DATAFLOW_ID bigint not null, FK_PIPELINE_ID bigint not null, FK_PROPDATA_ID bigint not null, FK_SAMPLING_ID bigint not null, primary key (`PID`), unique (`NAME`)) ENGINE=InnoDB;
create table `MODELQUALITY_ANALYTIC_TEST` (`PID` bigint not null auto_increment unique, `ANALYTIC_TEST_TAG` varchar(255), `ANALYTIC_TEST_TYPE` integer not null, `IS_EXECUTED` boolean not null, `NAME` varchar(255) not null unique, primary key (`PID`), unique (`NAME`)) ENGINE=InnoDB;
create table `MODELQUALITY_AP_TEST_AP_PIPELINE` (AP_TEST_ID bigint not null, AP_PIPELINE_ID bigint not null) ENGINE=InnoDB;
create table `MODELQUALITY_AP_TEST_DATASET` (AP_TEST_ID bigint not null, DATASET_ID bigint not null) ENGINE=InnoDB;
create table `MODELQUALITY_DATAFLOW` (`PID` bigint not null auto_increment unique, `TRANSFORM_DEDUP_TYPE` integer, `MATCH` boolean not null, `NAME` varchar(255) not null unique, `TRANSFORM_GROUP` integer, `VERSION` integer, primary key (`PID`), unique (`NAME`)) ENGINE=InnoDB;
create table `MODELQUALITY_DATASET` (`PID` bigint not null auto_increment unique, `CUSTOMER_SPACE` varchar(255) not null, `TYPE` integer not null, `INDUSTRY` varchar(255) not null, `NAME` varchar(255) not null unique, `SCHEMA_INTERPRETATION` integer not null, `TEST_HDFS_PATH` varchar(255), `TRAINING_HDFS_PATH` varchar(2048), primary key (`PID`), unique (`NAME`)) ENGINE=InnoDB;
create table `MODELQUALITY_MODELCONFIG` (`PID` bigint not null auto_increment unique, `CONFIG_DATA` longblob not null, `CREATION_DATE` datetime not null, `DESCRIPTION` varchar(4000), `NAME` varchar(255) not null, `UPDATE_DATE` datetime not null, primary key (`PID`)) ENGINE=InnoDB;
create table `MODELQUALITY_MODELRUN` (`PID` bigint not null auto_increment unique, `ANALYTIC_TEST_NAME` varchar(255), `ANALYTIC_TEST_TAG` varchar(255), `CREATION_DATE` datetime not null, `DESCRIPTION` varchar(4000), `ERROR_MESSAGE` varchar(4000), `MODEL_ID` varchar(255), `NAME` varchar(255) not null unique, `STATUS` integer not null, `UPDATE_DATE` datetime not null, FK_ANALYTIC_PIPELINE_ID bigint not null, FK_DATASET_ID bigint not null, primary key (`PID`), unique (`NAME`)) ENGINE=InnoDB;
create table `MODELQUALITY_PIPELINE_PIPELINE_STEP` (`STEP_ORDER` integer, PIPELINE_ID bigint, PIPELINE_STEP_ID bigint, primary key (PIPELINE_ID, PIPELINE_STEP_ID)) ENGINE=InnoDB;
create table `MODELQUALITY_PIPELINE_PROPERTY_DEF` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null, FK_PIPELINE_STEP_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `MODELQUALITY_PIPELINE_PROPERTY_VALUE` (`PID` bigint not null auto_increment unique, `VALUE` varchar(255) not null, FK_PIPELINE_PROPDEF_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `MODELQUALITY_PIPELINE_STEP` (`PID` bigint not null auto_increment unique, `LOAD_FROM_HDFS` boolean, `MAIN_CLASS_NAME` varchar(255) not null, `NAME` varchar(255) not null, `NAMED_PARAMS_TO_INIT` varchar(2000), `OPERATES_ON_COLUMNS` varchar(255), `RTS_SCRIPT` varchar(255), `SCRIPT` varchar(255) not null unique, primary key (`PID`), unique (`NAME`)) ENGINE=InnoDB;
create table `MODELQUALITY_PIPELINE` (`PID` bigint not null auto_increment unique, `DESCRIPTION` varchar(255), `NAME` varchar(255) not null unique, `PIPELINE_DRIVER` varchar(255), `PIPELINE_LIB_SCRIPT` varchar(255), `PIPELINE_SCRIPT` varchar(255), `VERSION` integer, primary key (`PID`), unique (`NAME`)) ENGINE=InnoDB;
create table `MODELQUALITY_PROPDATA` (`PID` bigint not null auto_increment unique, `DATA_CLOUD_VERSION` varchar(255) not null, `EXCLUDE_PROPDATA_COLUMNS` boolean, `EXCLUDE_PUBLIC_DOMAINS` boolean, `NAME` varchar(255) not null unique, `PREDEFINED_SELECTION_NAME` varchar(255), `VERSION` integer, primary key (`PID`), unique (`NAME`)) ENGINE=InnoDB;
create table `MODELQUALITY_SAMPLING_PROPERTY_DEF` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null, FK_SAMPLING_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `MODELQUALITY_SAMPLING_PROPERTY_VALUE` (`PID` bigint not null auto_increment unique, `VALUE` varchar(255), FK_SAMPLING_PROPDEF_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `MODELQUALITY_SAMPLING` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null unique, `PARALLEL_ENABLED` boolean not null, `VERSION` integer, primary key (`PID`), unique (`NAME`)) ENGINE=InnoDB;
create table `MODELQUALITY_SCORING_DATASET` (`PID` bigint not null auto_increment unique, `DATA_PATH` varchar(255), `NAME` varchar(255) not null unique, FK_DATASET_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `MODELREVIEW_COLUMNRESULT` (`PID` bigint not null auto_increment unique, `RULENAME` varchar(255) not null, `MODEL_ID` varchar(255) not null, `FLAGGED_COLUMNS` longblob, FK_TENANT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `MODELREVIEW_DATARULE` (`PID` bigint not null auto_increment unique, `DESCRIPTION` varchar(4000) not null, `DISPLAY_NAME` varchar(255) not null, `ENABLED` boolean not null, `FLAGGED_COLUMNS` longblob, `MANDATORY_REMOVAL` boolean not null, `NAME` varchar(255) not null, `PROPERTIES` longblob, FK_TABLE_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `MODELREVIEW_ROWRESULT` (`PID` bigint not null auto_increment unique, `RULENAME` varchar(255) not null, `MODEL_ID` varchar(255) not null, `FLAGGED_ROW_TO_COLUMNS` longblob, `FLAGGED_ROW_TO_POSITIVEEVENT` longblob, FK_TENANT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `MODEL_DEFINITION` (`PID` bigint not null auto_increment unique, `NAME` varchar(255) not null, primary key (`PID`)) ENGINE=InnoDB;
create table `MODEL_SUMMARY_DOWNLOAD_FLAGS` (`PID` bigint not null auto_increment, `MARK_TIME` datetime, `Tenant_ID` varchar(255), primary key (`PID`)) ENGINE=InnoDB;
create table `MODEL_SUMMARY_PROVENANCE_PROPERTY` (`PID` bigint not null auto_increment unique, `OPTION` varchar(255) not null, `VALUE` varchar(2048), MODEL_SUMMARY_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `MODEL_SUMMARY` (`PID` bigint not null auto_increment unique, `APPLICATION_ID` varchar(255), `CONSTRUCTION_TIME` bigint not null, `CROSS_VALIDATION_MEAN` double precision, `CROSS_VALIDATION_STD` double precision, `DISPLAY_NAME` varchar(255), `DOWNLOADED` boolean not null, `EVENT_TABLE_NAME` varchar(255), `ID` varchar(255) not null unique, `INCOMPLETE` boolean not null, `LAST_UPDATE_TIME` bigint not null, `LOOKUP_ID` varchar(255) not null, `MODEL_TYPE` varchar(255) not null, `MODULE_NAME` varchar(255), `NAME` varchar(255) not null, `PIVOT_ARTIFACT_PATH` varchar(2048), `ROC_SCORE` double precision not null, `SOURCE_SCHEMA_INTERPRETATION` varchar(255), `STATUS` integer not null, `TENANT_ID` bigint not null, `TEST_CONVERSION_COUNT` bigint not null, `TEST_ROW_COUNT` bigint not null, `TOP_10_PCT_LIFT` double precision, `TOP_20_PCT_LIFT` double precision, `TOP_30_PCT_LIFT` double precision, `TOTAL_CONVERSION_COUNT` bigint not null, `TOTAL_ROW_COUNT` bigint not null, `TRAINING_CONVERSION_COUNT` bigint not null, `TRAINING_ROW_COUNT` bigint not null, `TRAINING_TABLE_NAME` varchar(255), `TRANSFORMATION_GROUP_NAME` varchar(255), `UPLOADED` boolean not null, FK_KEY_VALUE_ID bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`ID`), unique (`NAME`, `TENANT_ID`)) ENGINE=InnoDB;
create table `MODEL` (`MODEL_PID` bigint not null auto_increment unique, `CUSTOMER` varchar(255), `DATA_FORMAT` varchar(255), `DATA_HDFS_PATH` varchar(2048), `DISPLAY_NAME` varchar(255), `FEATURES` longtext, `MODEL_ID` varchar(255), `KEYCOLS` varchar(500), `METADATA_HDFS_PATH` varchar(2048), `METADATA_TABLE` varchar(255), `MODEL_HDFS_DIR` varchar(2048), `NAME` varchar(255), `PROVENANCE_PROPERTIES` varchar(2048), `SCHEMA_HDFS_PATH` varchar(255), `TABLE_NAME` varchar(255), `TARGETS` varchar(255), FK_MODEL_DEF_ID bigint, primary key (`MODEL_PID`)) ENGINE=InnoDB;
create table `OAUTH2_ACCESS_TOKEN` (`PID` bigint not null auto_increment unique, `ACCESS_TOKEN` varchar(255) not null, `APP_ID` varchar(255), `TENANT_ID` bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`TENANT_ID`, `APP_ID`)) ENGINE=InnoDB;
create table `PREDICTOR_ELEMENT` (`PID` bigint not null auto_increment unique, `CORRELATION_SIGN` integer not null, `COUNT` bigint not null, `LIFT` double precision not null, `LOWER_INCLUSIVE` double precision, `NAME` varchar(255) not null, `TENANT_ID` bigint not null, `UNCERTAINTY_COEFF` double precision not null, `UPPER_EXCLUSIVE` double precision, `VALUES` longtext, `VISIBLE` boolean not null, FK_PREDICTOR_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `PREDICTOR` (`PID` bigint not null auto_increment unique, `APPROVED_USAGE` varchar(255), `CATEGORY` varchar(255), `DISPLAY_NAME` varchar(255), `RF_FEATURE_IMPORTANCE` double precision, `FUNDAMENTAL_TYPE` varchar(255), `NAME` varchar(255) not null, `TENANT_ID` bigint not null, `UNCERTAINTY_COEFF` double precision, `USED_FOR_BI` boolean not null, FK_MODELSUMMARY_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `PROSPECT_DISCOVERY_OPTION` (`PID` bigint not null auto_increment unique, `OPTION` varchar(255) not null, `TENANT_ID` bigint not null, `VALUE` varchar(255), primary key (`PID`)) ENGINE=InnoDB;
create table `QUOTA` (`PID` bigint not null auto_increment unique, `BALANCE` integer not null, `ID` varchar(255) not null unique, `TENANT_ID` bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `REPORT` (`PID` bigint not null auto_increment unique, `CREATED` datetime not null, `IS_OUT_OF_DATE` boolean not null, `NAME` varchar(255) not null, `PURPOSE` varchar(255) not null, `TENANT_ID` bigint not null, `UPDATED` datetime not null, FK_KEY_VALUE_ID bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`NAME`, `TENANT_ID`)) ENGINE=InnoDB;
create table `SALESFORCE_URL` (`PID` bigint not null auto_increment unique, `URL` varchar(255) not null, `NAME` varchar(255) not null unique, primary key (`PID`)) ENGINE=InnoDB;
create table `SEGMENT` (`PID` bigint not null auto_increment unique, `MODEL_ID` varchar(255), `NAME` varchar(255) not null, `PRIORITY` integer not null, `TENANT_ID` bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`NAME`, `TENANT_ID`)) ENGINE=InnoDB;
create table `SELECTED_ATTRIBUTE` (`PID` bigint not null auto_increment unique, `COLUMN_ID` varchar(100) not null, `IS_PREMIUM` boolean not null, `TENANT_PID` bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`TENANT_PID`, `COLUMN_ID`)) ENGINE=InnoDB;
create table `SOURCE_FILE` (`PID` bigint not null auto_increment unique, `APPLICATION_ID` varchar(255), `CREATED` datetime, `DESCRIPTION` varchar(255), `DISPLAY_NAME` varchar(255), `ENTITY_EXTERNAL_TYPE` varchar(255), `NAME` varchar(255) not null, `PATH` varchar(2048) not null, `SCHEMA_INTERPRETATION` varchar(255), `STATE` varchar(255), `TABLE_NAME` varchar(255), `TENANT_ID` bigint not null, `UPDATED` datetime, FK_TENANT_ID bigint not null, primary key (`PID`), unique (`NAME`, `TENANT_ID`)) ENGINE=InnoDB;
create table `TARGET_MARKET_DATA_FLOW_OPTION` (`PID` bigint not null auto_increment unique, `OPTION` varchar(255) not null, `VALUE` varchar(255), TARGET_MARKET_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `TARGET_MARKET_REPORT_MAP` (`PID` bigint not null auto_increment unique, REPORT_ID bigint not null, TARGET_MARKET_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `TARGET_MARKET` (`PID` bigint not null auto_increment unique, `ACCOUNT_FILTER` varchar(255), `APPLICATION_ID` varchar(255), `CONTACT_FILTER` varchar(255), `CREATION_TIMESTAMP` bigint not null, `DESCRIPTION` varchar(255) not null, `EVENT_COLUMN_NAME` varchar(255), `IS_DEFAULT` boolean, `MODEL_ID` varchar(255), `NAME` varchar(255) not null, `NUM_PROSPECTS_DESIRED` integer, `OFFSET` integer not null, `SELECTED_INTENT` varchar(255), `TENANT_ID` bigint not null, FK_TENANT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `TENANT_DEPLOYMENT` (`PID` bigint not null auto_increment unique, `CREATE_TIME` datetime not null, `CREATED_BY` varchar(255) not null, `CURRENT_LAUNCH_ID` bigint, `MESSAGE` longtext, `MODEL_ID` varchar(255), `STATUS` integer not null, `STEP` integer not null, `TENANT_ID` bigint not null, primary key (`PID`)) ENGINE=InnoDB;
create table `TENANT` (`TENANT_PID` bigint not null auto_increment unique, `EXTERNAL_USER_EMAIL_SENT` boolean, `TENANT_ID` varchar(255) not null unique, `NAME` varchar(255) not null unique, `REGISTERED_TIME` bigint not null, `UI_VERSION` varchar(255) not null, primary key (`TENANT_PID`)) ENGINE=InnoDB;
create table `THROTTLE_CONFIGURATION` (`PID` bigint not null auto_increment unique, `ENABLED` boolean not null, `IMMEDIATE` boolean not null, `JOB_RANK_CUTOFF` integer not null, `TIMESTAMP` datetime not null, primary key (`PID`)) ENGINE=InnoDB;
create table `WORKFLOW_JOB` (`PID` bigint not null auto_increment unique, `APPLICATION_ID` varchar(255), `ERROR_DETAILS` longtext, `INPUT_CONTEXT` varchar(4000), `OUTPUT_CONTEXT` varchar(4000), `REPORT_CONTEXT` varchar(4000), `START_TIME` bigint, `STATUS` varchar(255), `TENANT_ID` bigint not null, `USER_ID` varchar(255), `WORKFLOW_ID` bigint, FK_TENANT_ID bigint not null, primary key (`PID`)) ENGINE=InnoDB;

DROP TABLE IF EXISTS WORKFLOW_STEP_EXECUTION_CONTEXT ;
DROP TABLE IF EXISTS WORKFLOW_JOB_EXECUTION_CONTEXT ;
DROP TABLE IF EXISTS WORKFLOW_STEP_EXECUTION ;
DROP TABLE IF EXISTS WORKFLOW_JOB_EXECUTION_PARAMS ;
DROP TABLE IF EXISTS WORKFLOW_JOB_EXECUTION ;
DROP TABLE IF EXISTS WORKFLOW_JOB_INSTANCE ;

DROP TABLE IF EXISTS WORKFLOW_STEP_EXECUTION_SEQ ;
DROP TABLE IF EXISTS WORKFLOW_JOB_EXECUTION_SEQ ;
DROP TABLE IF EXISTS WORKFLOW_JOB_SEQ ;

CREATE TABLE WORKFLOW_JOB_INSTANCE  (
	JOB_INSTANCE_ID BIGINT  NOT NULL PRIMARY KEY ,
	VERSION BIGINT ,
	JOB_NAME VARCHAR(100) NOT NULL,
	JOB_KEY VARCHAR(32) NOT NULL,
	constraint JOB_INST_UN unique (JOB_NAME, JOB_KEY)
) ENGINE=InnoDB;

CREATE TABLE WORKFLOW_JOB_EXECUTION  (
	JOB_EXECUTION_ID BIGINT  NOT NULL PRIMARY KEY ,
	VERSION BIGINT  ,
	JOB_INSTANCE_ID BIGINT NOT NULL,
	CREATE_TIME DATETIME NOT NULL,
	START_TIME DATETIME DEFAULT NULL ,
	END_TIME DATETIME DEFAULT NULL ,
	STATUS VARCHAR(10) ,
	EXIT_CODE VARCHAR(2500) ,
	EXIT_MESSAGE VARCHAR(2500) ,
	LAST_UPDATED DATETIME,
	JOB_CONFIGURATION_LOCATION VARCHAR(2500) NULL
) ENGINE=InnoDB;

CREATE TABLE WORKFLOW_JOB_EXECUTION_PARAMS  (
	PID BIGINT  NOT NULL auto_increment unique PRIMARY KEY ,
	JOB_EXECUTION_ID BIGINT NOT NULL ,
	TYPE_CD VARCHAR(6) NOT NULL ,
	KEY_NAME TEXT NOT NULL ,
	STRING_VAL MEDIUMTEXT ,
	DATE_VAL DATETIME DEFAULT NULL ,
	LONG_VAL BIGINT ,
	DOUBLE_VAL DOUBLE PRECISION ,
	IDENTIFYING CHAR(1) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE WORKFLOW_STEP_EXECUTION  (
	STEP_EXECUTION_ID BIGINT  NOT NULL PRIMARY KEY ,
	VERSION BIGINT NOT NULL,
	STEP_NAME VARCHAR(100) NOT NULL,
	JOB_EXECUTION_ID BIGINT NOT NULL,
	START_TIME DATETIME NOT NULL ,
	END_TIME DATETIME DEFAULT NULL ,
	STATUS VARCHAR(10) ,
	COMMIT_COUNT BIGINT ,
	READ_COUNT BIGINT ,
	FILTER_COUNT BIGINT ,
	WRITE_COUNT BIGINT ,
	READ_SKIP_COUNT BIGINT ,
	WRITE_SKIP_COUNT BIGINT ,
	PROCESS_SKIP_COUNT BIGINT ,
	ROLLBACK_COUNT BIGINT ,
	EXIT_CODE VARCHAR(2500) ,
	EXIT_MESSAGE VARCHAR(2500) ,
	LAST_UPDATED DATETIME
) ENGINE=InnoDB;

CREATE TABLE WORKFLOW_STEP_EXECUTION_CONTEXT  (
	STEP_EXECUTION_ID BIGINT NOT NULL PRIMARY KEY,
	SHORT_CONTEXT VARCHAR(2500) NOT NULL,
	SERIALIZED_CONTEXT TEXT
) ENGINE=InnoDB;

CREATE TABLE WORKFLOW_JOB_EXECUTION_CONTEXT  (
	JOB_EXECUTION_ID BIGINT NOT NULL PRIMARY KEY,
	SHORT_CONTEXT VARCHAR(2500) NOT NULL,
	SERIALIZED_CONTEXT MEDIUMTEXT
) ENGINE=InnoDB;

CREATE TABLE WORKFLOW_STEP_EXECUTION_SEQ (
	ID BIGINT NOT NULL,
	UNIQUE_KEY CHAR(1) NOT NULL,
	constraint UNIQUE_KEY_UN unique (UNIQUE_KEY)
) ENGINE=InnoDB;

CREATE TABLE WORKFLOW_JOB_EXECUTION_SEQ (
	ID BIGINT NOT NULL,
	UNIQUE_KEY CHAR(1) NOT NULL,
	constraint UNIQUE_KEY_UN unique (UNIQUE_KEY)
) ENGINE=InnoDB;

CREATE TABLE WORKFLOW_JOB_SEQ (
	ID BIGINT NOT NULL,
	UNIQUE_KEY CHAR(1) NOT NULL,
	constraint UNIQUE_KEY_UN unique (UNIQUE_KEY)
) ENGINE=InnoDB;

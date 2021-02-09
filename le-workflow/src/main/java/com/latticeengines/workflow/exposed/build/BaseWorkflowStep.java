package com.latticeengines.workflow.exposed.build;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.client.YarnClient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.WorkflowConfigurationUtils;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.app.LatticeInsightsInternalProxy;
import com.latticeengines.proxy.exposed.dataplatform.JobProxy;
import com.latticeengines.proxy.exposed.dataplatform.ModelProxy;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.pls.EmailProxy;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.WorkflowReportService;
import com.latticeengines.workflow.exposed.util.WorkflowUtils;

import avro.shaded.com.google.common.collect.Sets;

public abstract class BaseWorkflowStep<T extends BaseStepConfiguration> extends AbstractStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseWorkflowStep.class);

    protected static final String PREMATCH_EVENT_TABLE = "PREMATCH_EVENT_TABLE";
    protected static final String PREMATCH_UPSTREAM_EVENT_TABLE = "PREMATCH_UPSTREAM_EVENT_TABLE";
    protected static final String FILTER_EVENT_TABLE = "FILTER_EVENT_TABLE";
    protected static final String PREMATCH_EVENT_TABLE_NAME = "PREMATCH_EVENT_TABLE_NAME";
    protected static final String EVENT_TABLE_EVENT_COLUMN = "EVENT_TABLE_EVENT_COLUMN";
    protected static final String FILTER_EVENT_TARGET_TABLE_NAME = "FILTER_EVENT_TARGET_TABLE_NAME";
    protected static final String EVENT_TABLE = "EVENT_TABLE";
    protected static final String EVENT_COLUMN = "EVENT_COLUMN";
    protected static final String DB_CREDS = "DB_CREDS";
    protected static final String MATCH_COMMAND_ID = "MATCH_COMMAND_ID";
    protected static final String MATCH_COMMAND = "MATCH_COMMAND";
    protected static final String MATCH_TABLE = "MATCH_TABLE";
    protected static final String MATCH_INPUT_ID_COLUMN = "MATCH_INPUT_ID_COLUMN";
    protected static final String MATCH_RESULT_TABLE = "MATCH_RESULT_TABLE";
    protected static final String MATCH_RESULT_TABLE_NAME = "MATCH_RESULT_TABLE_NAME";
    protected static final String MATCH_RESULT_MATCH_COUNT = "MATCH_RESULT_MATCH_COUNT";
    protected static final String MATCH_RESULT_PUBLIC_DOMAIN_COUNT = "MATCH_RESULT_PUBLIC_DOMAIN_COUNT";
    protected static final String MATCH_RESULT_TOTAL_COUNT = "MATCH_RESULT_TOTAL_COUNT";
    protected static final String MATCH_RESULT_MATCH_RATE = "MATCH_RESULT_MATCH_RATE";
    protected static final String MATCH_RESULT_PUBLIC_DOMAIN_RATE = "MATCH_RESULT_PUBLIC_DOMAIN_RATE";
    protected static final String MATCH_CANDIDATES_TABLE_NAME = "MATCH_CANDIDATES_TABLE_NAME";
    protected static final String MATCH_PREDEFINED_SELECTION = "MATCH_PREDEFINED_SELECTION";
    protected static final String MATCH_PREDEFINED_SELECTION_VERSION = "MATCH_PREDEFINED_SELECTION_VERSION";
    protected static final String MATCH_CUSTOMIZED_SELECTION = "MATCH_CUSTOMIZED_SELECTION";
    protected static final String MODELING_SERVICE_EXECUTOR_BUILDER = "MODELING_SERVICE_EXECUTOR_BUILDER";
    protected static final String MODEL_APP_IDS = "MODEL_APP_IDS";
    protected static final String MODEL_AVG_PROBABILITY = "MODEL_AVG_PROBABILITY";
    protected static final String SCORING_AVG_SCORE = "SCORING_AVG_SCORE";
    protected static final String SCORING_AVG_SCORES = "SCORING_AVG_SCORES";
    protected static final String LIFT_SCORE_FIELDS = "LIFT_SCORE_FIELDS";
    protected static final String SCORING_BUCKET_METADATA = "SCORING_BUCKET_METADATA";
    protected static final String SCORING_RESULT_TABLE_NAME = "SCORING_RESULT_TABLE_NAME";
    protected static final String SCORING_MODEL_ID = "SCORING_MODEL_ID";
    protected static final String SCORING_MODEL_ID_P2 = "SCORING_MODEL_ID_P2";
    protected static final String SCORING_MODEL_ID_P3 = "SCORING_MODEL_ID_P3";
    protected static final String SCORING_MODEL_TYPE = "SCORING_MODEL_TYPE";
    protected static final String SCORING_SOURCE_DIR = "SCORING_SOURCE_DIR";
    protected static final String SCORING_UNIQUEKEY_COLUMN = "SCORING_UNIQUEKEY_COLUMN";
    protected static final String ATTR_LEVEL_TYPE = "ATTR_LEVEL_TYPE";
    protected static final String IMPORT_DATA_APPLICATION_ID = "IMPORT_DATA_APPLICATION_ID";
    protected static final String IMPORT_DATA_LOCATION = "IMPORT_DATA_LOCATION";
    protected static final String ACTIVATE_MODEL_IDS = "ACTIVATE_MODEL_IDS";
    protected static final String COMPUTE_LIFT_INPUT_TABLE_NAME = "COMPUTE_LIFT_INPUT_TABLE_NAME";
    protected static final String MAP_TARGET_SCORE_INPUT_TABLE_NAME = "MAP_TARGET_SCORE_INPUT_TABLE_NAME";
    protected static final String PIVOT_SCORE_INPUT_TABLE_NAME = "PIVOT_SCORE_INPUT_TABLE_NAME";
    protected static final String EXPORT_DATA_APPLICATION_ID = "EXPORT_DATA_APPLICATION_ID";
    protected static final String SKIP_EXPORT_DATA = "SKIP_EXPORT_DATA";
    protected static final String EXPORT_TABLE_NAME = "EXPORT_TABLE_NAME";
    protected static final String EXPORT_MERGE_FILE_NAME = "EXPORT_MERGE_FILE_NAME";
    protected static final String EXPORT_MERGE_FILE_PATH = "EXPORT_MERGE_FILE_PATH";
    protected static final String EXPORT_INPUT_PATH = "EXPORT_INPUT_PATH";
    protected static final String EXPORT_OUTPUT_PATH = "EXPORT_OUTPUT_PATH";
    protected static final String EXPORT_BUCKET_TOOL_TABLE_NAME = "EXPORT_BUCKET_TOOL_TABLE_NAME";
    protected static final String EXPORT_BUCKET_TOOL_OUTPUT_PATH = "EXPORT_BUCKET_TOOL_OUTPUT_PATH";
    protected static final String EXPORT_SCORE_TRAINING_FILE_TABLE_NAME = "EXPORT_SCORE_TRAINING_FILE_TABLE_NAME";
    protected static final String EXPORT_SCORE_TRAINING_FILE_OUTPUT_PATH = "EXPORT_SCORE_TRAINING_FILE_OUTPUT_PATH";
    protected static final String TRANSFORMATION_GROUP_NAME = "TRANSFORMATION_GROUP_NAME";
    protected static final String COLUMN_RULE_RESULTS = "COLUMN_RULE_RESULTS";
    protected static final String ROW_RULE_RESULTS = "ROW_RULE_RESULTS";
    protected static final String EVENT_TO_MODELID = "EVENT_TO_MODELID";
    protected static final String DATA_RULES = "DATA_RULES";
    protected static final String SOURCE_IMPORT_TABLE = "SOURCE_IMPORT_TABLE_NAME";
    protected static final String SOURCE_FILE_PATH = "SOURCE_FILE_PATH";
    protected static final String TRANSFORM_PIPELINE_VERSION = "TRANSFORM_PIPELINE_VERSION";
    protected static final String EVENT_COUNTER_MAP = "EVENT_COUNTER_MAP";
    protected static final String ENTITY_VALIDATION_SUMMARY = "ENTITY_VALIDATION_SUMMARY";
    protected static final String IMPORT_FILE_SIGNATURE = "IMPORT_FILE_SIGNATURE";

    // DCP
    protected static final String UPLOAD_STATS = "UPLOAD_STATS";
    protected static final String DUNS_COUNT_TABLE_NAME = "DUNS_COUNT_TABLE_NAME";
    protected static final String DUNS_COUNT_TABLE_NAMES = "DUNS_COUNT_TABLE_NAMES";
    protected static final String INPUT_PRESENCE_REPORT = "INPUT_PRESENCE_REPORT";

    // CDL
    public static final String CONSOLIDATE_INPUT_TEMPLATES = "CONSOLIDATE_INPUT_TEMPLATES";
    public static final String CONSOLIDATE_TEMPLATES_IN_ORDER = "CONSOLIDATE_TEMPLATES_IN_ORDER";
    public static final String CONSOLIDATE_INPUT_IMPORTS = "CONSOLIDATE_INPUT_IMPORTS";
    public static final String SOFT_DELETE_ACTIONS = "SOFT_DEELETE_ACTIONS";
    public static final String HARD_DELETE_ACTIONS = "HARD_DEELETE_ACTIONS";
    public static final String CDL_ACTIVE_VERSION = "CDL_ACTIVE_VERSION";
    public static final String CUSTOMER_SPACE = "CUSTOMER_SPACE";
    public static final String TIMELINE_RAWTABLES_GOING_TO_DYNAMO = "TIMELINE_RAWTABLES_GOING_TO_DYNAMO";
    public static final String ATLAS_ACCOUNT_LOOKUP_TO_DYNAMO = "ATLAS_ACCOUNT_LOOKUP_TO_DYNAMO";
    public static final String TABLES_GOING_TO_DYNAMO = "TABLES_GOING_TO_DYNAMO";
    public static final String TABLES_GOING_TO_ES = "TABLES_GOING_TO_ES";
    public static final String TABLES_GOING_TO_REDSHIFT = "TABLES_GOING_TO_REDSHIFT";
    public static final String ENTITIES_WITH_SCHEMA_CHANGE = "ENTITIES_WITH_SCHEMA_CHANGE";
    public static final String RATING_MODELS = "RATING_MODELS";
    public static final String ACTION_IMPACTED_SEGMENTS = "ACTION_IMPACTED_SEGMENTS";
    public static final String ACTION_IMPACTED_ENGINES = "ACTION_IMPACTED_ENGINES";
    public static final String ACCOUNT_LOOKUP_DATA_UNIT_NAME = "AccountLookup";
    public static final String RESCORE_ALL_RATINGS = "RESCORE_ALL_RATINGS";
    public static final String CURRENT_RATING_ITERATION = "CURRENT_RATING_ITERATION";
    public static final String INACTIVE_ENGINE_ATTRIBUTES = "INACTIVE_ENGINE_ATTRIBUTES";
    public static final String INACTIVE_ENGINES = "INACTIVE_ENGINES";
    public static final String ITERATION_RATING_MODELS = "ITERATION_RATING_MODELS";
    public static final String ITERATION_AI_RATING_MODELS = "ITERATION_AI_RATING_MODELS";
    public static final String PYTHON_MAJOR_VERSION = "PYTHON_MAJOR_VERSION";
    public static final String RATING_MODELS_BY_ITERATION = "RATING_MODELS_BY_ITERATION";
    protected static final String IMPACTED_ENTITIES = "IMPACTED_ENTITIES";
    protected static final String ITERATION_INACTIVE_ENGINES = "ITERATION_INACTIVE_ENGINES";
    protected static final String INACTIVE_RATINGS_TABLE_NAME = "INACTIVE_RATINGS_TABLE_NAME";
    protected static final String RATING_ENGINE_ID_TO_ACTIVATE = "RATING_ENGINE_ID_TO_ACTIVATE";
    protected static final String RULE_RAW_RATING_TABLE_NAME = "RULE_RAW_RATING_TABLE_NAME";
    protected static final String AI_RAW_RATING_TABLE_NAME = "AI_RAW_RATING_TABLE_NAME";
    protected static final String AI_RAW_RATING_TABLE_NAME_P2 = "AI_RAW_RATING_TABLE_NAME_P2";
    protected static final String PIVOTED_RATINGS_TABLE_NAME = "PIVOTED_RATINGS_TABLE_NAME";
    protected static final String RATING_ITERATION_RESULT_TABLE_NAME = "RATING_ITERATION_RESULT_TABLE_NAME";
    public static final String CDL_INACTIVE_VERSION = "CDL_INACTIVE_VERSION";
    protected static final String CDL_EVALUATION_DATE = "CDL_EVALUATION_DATE";
    public static final String CDL_COLLECTION_STATUS = "CDL_COLLECTION_STATUS";
    public static final String HAS_DATA_CLOUD_MAJOR_CHANGE = "HAS_DATA_CLOUD_MAJOR_CHANGE";
    public static final String HAS_DATA_CLOUD_MINOR_CHANGE = "HAS_DATA_CLOUD_MINOR_CHANGE";
    public static final String SYSTEM_ACTION_IDS = "SYSTEM_ACTION_IDS";
    protected static final String PA_TIMESTAMP = WorkflowContextConstants.Inputs.PA_TIMESTAMP;
    protected static final String NEW_RECORD_CUT_OFF_TIME = "NEW_RECORD_CUT_OFF_TIME";
    public static final String PA_SKIP_ENTITIES = "PA_SKIP_ENTITIES";
    protected static final String CLEANUP_TIMESTAMP = "CLEANUP_TIMESTAMP";
    protected static final String STATS_TABLE_NAMES = "STATS_TABLE_NAMES";
    protected static final String TEMPORARY_CDL_TABLES = "TEMPORARY_CDL_TABLES";
    protected static final String ENTITY_DIFF_TABLES = "ENTITY_DIFF_TABLES";
    protected static final String ENTITY_CHANGELIST_TABLES = "ENTITY_CHANGELIST_TABLES";
    protected static final String NEED_PUBLISH_ACCOUNT_LOOKUP = "NEED_PUBLISH_ACCOUNT_LOOKUP";
    protected static final String ENTITY_REPORT_CHANGELIST_TABLES = "ENTITY_REPORT_CHANGELIST_TABLES";
    protected static final String PROCESSED_DIFF_TABLES = "PROCESSED_DIFF_TABLES";
    protected static final String RESET_ENTITIES = "RESET_ENTITIES";
    protected static final String EVALUATION_PERIOD = "EVALUATION_PERIOD";
    protected static final String HAS_CROSS_SELL_MODEL = "HAS_CROSS_SELL_MODEL";
    protected static final String RATING_LIFTS = "RATING_LIFTS";
    protected static final String BUCKETED_SCORE_SUMMARIES = "BUCKETED_SCORE_SUMMARIES";
    protected static final String BUCKETED_SCORE_SUMMARIES_AGG = "BUCKETED_SCORE_SUMMARIES_AGG";
    protected static final String BUCKET_METADATA_MAP_AGG = "BUCKET_METADATA_MAP_AGG";
    protected static final String MODEL_GUID_ENGINE_ID_MAP_AGG = "MODEL_GUID_ENGINE_ID_MAP_AGG";
    public static final String SKIP_PUBLISH_PA_TO_S3 = "SKIP_PUBLISH_PA_TO_S3";
    protected static final String ATLAS_EXPORT_DATA_UNIT = "ATLAS_EXPORT_DATA_UNIT";
    protected static final String ATLAS_EXPORT_DELETE_PATH = "ATLAS_EXPORT_DELETE_PATH";
    protected static final String PRIMARY_IMPORT_SYSTEM = "PRIMARY_IMPORT_SYSTEM";
    public static final String ADDED_ACCOUNTS_DELTA_TABLE = "ADDED_ACCOUNTS_DELTA_TABLE";
    public static final String REMOVED_ACCOUNTS_DELTA_TABLE = "REMOVED_ACCOUNTS_DELTA_TABLE";
    public static final String ADDED_CONTACTS_DELTA_TABLE = "ADDED_CONTACTS_DELTA_TABLE";
    public static final String REMOVED_CONTACTS_DELTA_TABLE = "REMOVED_CONTACTS_DELTA_TABLE";
    protected static final String ADDED_ACCOUNTS_FULL_CONTACTS_TABLE = "ADDED_ACCOUNTS_FULL_CONTACTS_TABLE";
    protected static final String ADDED_RECOMMENDATION_TABLE = "ADDED_RECOMMENDATION_TABLE";
    protected static final String DELETED_RECOMMENDATION_TABLE = "DELETED_RECOMMENDATION_TABLE";
    protected static final String FULL_ACCOUNTS_UNIVERSE = "FULL_ACCOUNTS_UNIVERSE";
    protected static final String FULL_CONTACTS_UNIVERSE = "FULL_CONTACTS_UNIVERSE";
    protected static final String PREVIOUS_ACCUMULATIVE_ACCOUNTS = "PREVIOUS_ACCUMULATIVE_ACCOUNTS";
    protected static final String PREVIOUS_ACCUMULATIVE_CONTACTS = "PREVIOUS_ACCUMULATIVE_CONTACTS";
    protected static final String ACCOUNTS_ADDED = "ACCOUNTS_ADDED";
    protected static final String ACCOUNTS_DELETED = "ACCOUNTS_DELETED";
    protected static final String ACCUMULATIVE_ACCOUNTS = "ACCUMULATIVE_ACCOUNTS";
    protected static final String CONTACTS_ADDED = "CONTACTS_ADDED";
    protected static final String CONTACTS_DELETED = "CONTACTS_DELETED";
    protected static final String FULL_CONTACTS = "FULL_CONTACTS";
    protected static final String ACCUMULATIVE_CONTACTS = "ACCUMULATIVE_CONTACTS";
    protected static final String PREVIOUS_ACCOUNTS_UNIVERSE = "PREVIOUS_ACCOUNTS_UNIVERSE";
    protected static final String PREVIOUS_CONTACTS_UNIVERSE = "PREVIOUS_CONTACTS_UNIVERSE";
    protected static final String FULL_LAUNCH_UNIVERSE = "FULL_LAUNCH_UNIVERSE";
    protected static final String ACCOUNTS_DATA_UNIT = "ACCOUNTS_DATA_UNIT";
    protected static final String CONTACTS_DATA_UNIT = "CONTACTS_DATA_UNIT";
    public static final String DELTA_TABLE_COUNTS = "DELTA_TABLE_COUNTS";
    protected static final String RECOMMENDATION_ACCOUNT_DISPLAY_NAMES = "RECOMMENDATION_ACCOUNT_DISPLAY_NAMES";
    public static final String RECOMMENDATION_CONTACT_DISPLAY_NAMES = "RECOMMENDATION_CONTACT_DISPLAY_NAMES";
    protected static final String PROCESS_ACCOUNT_STATS_MERGE = "PROCESS_ACCOUNT_STATS_MERGE";
    protected static final String PROCESS_ACCOUNT_FULL_PROFILE = "PROCESS_ACCOUNT_FULL_PROFILE";
    protected static final String ACTIVITY_STREAMS_NEED_REBUILD = "ACTIVITY_STREAMS_NEED_REBUILD";
    protected static final String ACTIVITY_STREAMS_RELINK = "ACTIVITY_STREAMS_RELINK";
    protected static final String ACTIVITY_PARTITION_MIGRATION_PERFORMED = "ACTIVITY_PARTITION_MIGRATION_PERFORMED";
    protected static final String ACTIVITY_MIGRATED_RAW_STREAM = "ACTIVITY_MIGRATED_RAW_STREAM";
    protected static final String ACTIVITY_MIGRATED_DAILY_STREAM = "ACTIVITY_MIGRATED_DAILY_STREAM";
    protected static final String ACTIVITY_STREAMS_SKIP_AGG = "ACTIVITY_STREAMS_SKIP_AGG";
    protected static final String ACTIVITY_STREAM_METADATA_CACHE = "STREAM_METADATA_CACHE";
    protected static final String STREAM_DIMENSION_METADATA_MAP = "STREAM_DIMENSION_METADATA_MAP";
    protected static final String STREAM_DIMENSION_VALUE_ID_MAP = "STREAM_DIMENSION_VALUE_ID_MAP";
    protected static final String METRICS_GROUP_TABLE_FORMAT = "METRICS_GROUP_%s"; // groupId
    // set of merged activity metrics groups' serving entities
    protected static final String ACTIVITY_MERGED_METRICS_SERVING_ENTITIES = "MERGED_METRICS_SERVING_ENTITIES";
    protected static final String ACTIVITY_METRICS_CATEGORICAL_ATTR = "ACTIVITY_METRICS_CATEGORICAL_ATTR";
    protected static final String ACTIVITY_METRICS_CATEGORIES = "ACTIVITY_METRICS_CATEGORIES";
    protected static final String SCORE_TRAINING_FILE_INCLUDED_FEATURES = "SCORE_TRAINING_FILE_INCLUDED_FEATURES";
    // streamId, period
    protected static final String PERIOD_STORE_TABLE_FORMAT = "PERIODSTORE_%s_%s";
    protected static final String PERIOD_STORE_TABLE_NAME = "PERIOD_STORE_TABLE_NAME";
    protected static final String PERFORM_SOFT_DELETE = "PERFORM_SOFT_DELETE"; //
    protected static final String PERFORM_HARD_DELETE = "PERFORM_HARD_DELETE"; //
    protected static final String SOFT_DELETE_RECORD_COUNT = "SOFT_DELETE_RECORD_COUNT"; //
    protected static final String RAW_STREAM_TABLE_AFTER_DELETE = "RAW_STREAM_TABLE_AFTER_DELETE"; //
    protected static final String ACTIVITY_IMPORT_AFTER_HARD_DELETE = "ACTIVITY_IMPORT_AFTER_HARD_DELETE"; //
    protected static final String MERGED_PRODUCT_IMPORTS = "MERGED_PRODUCT_IMPORTS";
    protected static final String ROLLUP_PRODUCT_TABLE = "ROLLUP_PRODUCT_TABLE";
    protected static final String RETAIN_PRODUCT_TYPE = "RETAIN_PRODUCT_TYPE";
    protected static final String Raw_TXN_STREAMS = "Raw_TXN_STREAMS";
    protected static final String DAILY_TXN_STREAMS = "DAILY_TXN_STREAMS";
    protected static final String PERIOD_TXN_STREAMS = "PERIOD_TXN_STREAMS";
    // profile report

    // intermediate results for skippable steps
    protected static final String NEW_ENTITY_MATCH_ENVS = "NEW_ENTITY_MATCH_ENVS";
    protected static final String ENTITY_MATCH_COMPLETED = "ENTITY_MATCH_COMPLETED";
    protected static final String ENTITY_MATCH_ACCOUNT_TARGETTABLE = "ENTITY_MATCH_ACCOUNT_TARGETTABLE";
    protected static final String ENTITY_MATCH_CONTACT_TARGETTABLE = "ENTITY_MATCH_CONTACT_TARGETTABLE";
    protected static final String ENTITY_MATCH_TXN_TARGETTABLE = "ENTITY_MATCH_TXN_TARGETTABLE";
    public static final String ENTITY_MATCH_CONTACT_ACCOUNT_TARGETTABLE = "ENTITY_MATCH_CONTACT_ACCOUNT_TARGETTABLE";
    public static final String ENTITY_MATCH_TXN_ACCOUNT_TARGETTABLE = "ENTITY_MATCH_TXN_ACCOUNT_TARGETTABLE";
    protected static final String ACCOUNT_DIFF_TABLE_NAME = "ACCOUNT_DIFF_TABLE_NAME";
    protected static final String ACCOUNT_CHANGELIST_TABLE_NAME = "ACCOUNT_CHANGELIST_TABLE_NAME";
    protected static final String LATTICE_ACCOUNT_CHANGELIST_TABLE_NAME = "LATTICE_ACCOUNT_CHANGELIST_TABLE_NAME";
    protected static final String ACCOUNT_REPORT_CHANGELIST_TABLE_NAME = "ACCOUNT_REPORT_CHANGELIST_TABLE_NAME";
    protected static final String SYSTEM_ACCOUNT_MASTER_TABLE_NAME = "SYSTEM_ACCOUNT_MASTER_TABLE_NAME";
    protected static final String ACCOUNT_MASTER_TABLE_NAME = "ACCOUNT_MASTER_TABLE_NAME";
    protected static final String FULL_ACCOUNT_TABLE_NAME = "FULL_ACCOUNT_TABLE_NAME";
    protected static final String LATTICE_ACCOUNT_TABLE_NAME = "LATTICE_ACCOUNT_TABLE_NAME";
    protected static final String REBUILD_LATTICE_ACCOUNT = "REBUILD_LATTICE_ACCOUNT";
    protected static final String ACCOUNT_FEATURE_TABLE_NAME = "ACCOUNT_FEATURE_TABLE_NAME";
    protected static final String ACCOUNT_EXPORT_TABLE_NAME = "ACCOUNT_EXPORT_TABLE_NAME";
    protected static final String ACCOUNT_PROFILE_TABLE_NAME = "ACCOUNT_PROFILE_TABLE_NAME";
    protected static final String LATTICE_ACCOUNT_PROFILE_TABLE_NAME = "LATTICE_ACCOUNT_PROFILE_TABLE_NAME";
    protected static final String ACCOUNT_RE_PROFILE_ATTRS = "ACCOUNT_RE_PROFILE_ATTRS";
    protected static final String LATTICE_ACCOUNT_RE_PROFILE_ATTRS = "LATTICE_ACCOUNT_RE_PROFILE_ATTRS";
    protected static final String ACCOUNT_SERVING_TABLE_NAME = "ACCOUNT_SERVING_TABLE_NAME";
    protected static final String ACCOUNT_STATS_TABLE_NAME = "ACCOUNT_STATS_TABLE_NAME";
    protected static final String ACCOUNT_STATS_DIFF_TABLE_NAME = "ACCOUNT_STATS_DIFF_TABLE_NAME";
    protected static final String STATS_UPDATED = "STATS_UPDATED";
    protected static final String ACCOUNT_STATS_UPDATED = "ACCOUNT_STATS_UPDATED";
    protected static final String FULL_ACCOUNT_STATS_TABLE_NAME = "FULL_ACCOUNT_STATS_TABLE_NAME";
    protected static final String ACCOUNT_LOOKUP_TABLE_NAME = "ACCOUNT_LOOKUP_TABLE_NAME";
    protected static final String CONTACT_CHANGELIST_TABLE_NAME = "CONTACT_CHANGELIST_TABLE_NAME";
    protected static final String CONTACT_SERVING_TABLE_NAME = "CONTACT_SERVING_TABLE_NAME";
    protected static final String CONTACT_PROFILE_TABLE_NAME = "CONTACT_PROFILE_TABLE_NAME";
    protected static final String CONTACT_STATS_TABLE_NAME = "CONTACT_STATS_TABLE_NAME";
    protected static final String CONTACT_STATS_DIFF_TABLE_NAME = "CONTACT_STATS_DIFF_TABLE_NAME";
    protected static final String CONTACT_RE_PROFILE_ATTRS = "CONTACT_RE_PROFILE_ATTRS";
    protected static final String CONTACT_STATS_UPDATED = "CONTACT_STATS_UPDATED";
    protected static final String DAILY_TRXN_TABLE_NAME = "DAILY_TRXN_TABLE_NAME";
    protected static final String AGG_DAILY_TRXN_TABLE_NAME = "AGG_DAILY_TRXN_TABLE_NAME";
    protected static final String PERIOD_TRXN_TABLE_NAME = "PERIOD_TRXN_TABLE_NAME";
    protected static final String PERIOD_TRXN_TABLE_NAMES_BY_PERIOD_NAME = "PERIOD_TRXN_TABLE_NAMES_BY_PERIOD_NAME";
    protected static final String CATALOG_TABLE_NAME = "CATALOG_TABLE_NAME";
    protected static final String CATALOG_NEW_IMPORT = "CATALOG_NEW_IMPORT";
    protected static final String ENTITY_MATCH_STREAM_TARGETTABLE = "ENTITY_MATCH_STREAM_TARGETTABLE";
    public static final String ENTITY_MATCH_STREAM_ACCOUNT_TARGETTABLE = "ENTITY_MATCH_STREAM_ACCOUNT_TARGETTABLE";
    public static final String ENTITY_MATCH_STREAM_CONTACT_TARGETTABLE = "ENTITY_MATCH_STREAM_CONTACT_TARGETTABLE";
    protected static final String RAW_ACTIVITY_STREAM_TABLE_NAME = "RAW_ACTIVITY_STREAM_TABLE_NAME";
    protected static final String RAW_ACTIVITY_STREAM_DELTA_TABLE_NAME = "RAW_ACTIVITY_STREAM_DELTA_TABLE_NAME";
    protected static final String AGG_DAILY_ACTIVITY_STREAM_TABLE_NAME = "AGG_DAILY_ACTIVITY_STREAM_TABLE_NAME";
    protected static final String DAILY_ACTIVITY_STREAM_DELTA_TABLE_NAME = "DAILY_ACTIVITY_STREAM_DELTA_TABLE_NAME";
    protected static final String LAST_ACTIVITY_DATE_TABLE_NAME = "LAST_ACTIVITY_DATE_TABLE_NAME";
    protected static final String METRICS_GROUP_TABLE_NAME = "METRICS_GROUP_TABLE_NAME";
    protected static final String MERGED_METRICS_GROUP_TABLE_NAME = "MERGED_METRICS_GROUP_TABLE_NAME";
    protected static final String AGG_PERIOD_TRXN_TABLE_NAME = "AGG_PERIOD_TRXN_TABLE_NAME";
    protected static final String SPENDING_ANALYSIS_PERIOD_TABLE_NAME = "SPENDING_ANALYSIS_PERIOD_TABLE_NAME";
    protected static final String TIMELINE_MASTER_TABLE_NAME = "TIMELINE_MASTER_TABLE_NAME";
    protected static final String TIMELINE_DIFF_TABLE_NAME = "TIMELINE_DIFF_TABLE_NAME";
    protected static final String TIMELINE_REBUILD = "TIMELINE_REBUILD";
    protected static final String JOURNEY_STAGE_TABLE_NAME = "JOURNEY_STAGE_TABLE_NAME";
    protected static final String ACTIVITY_ALERT_GENERATED = "ALERT_GENERATED";
    protected static final String ACTIVITY_ALERT_PUBLISHED = "ALERT_PUBLISHED";
    protected static final String ACTIVITY_ALERT_MASTER_TABLE_NAME = "ALERT_MASTER_TABLE_NAME";
    protected static final String ACTIVITY_ALERT_DIFF_TABLE_NAME = "ALERT_DIFF_TABLE_NAME";
    protected static final String INTENT_ALERT_NEW_ACCOUNT_TABLE_NAME = "INTENT_ALERT_NEW_ACCOUNT_TABLE_NAME";
    protected static final String INTENT_ALERT_ALL_ACCOUNT_TABLE_NAME = "INTENT_ALERT_ALL_ACCOUNT_TABLE_NAME";

    protected static final String PH_SERVING_TABLE_NAME = "PH_SERVING_TABLE_NAME";
    protected static final String PH_PROFILE_TABLE_NAME = "PH_PROFILE_TABLE_NAME";
    protected static final String PH_STATS_TABLE_NAME = "PH_STATS_TABLE_NAME";
    protected static final String PH_DEPIVOTED_TABLE_NAME = "PH_DEPIVOTED_TABLE_NAME";
    protected static final String CURATED_ACCOUNT_SERVING_TABLE_NAME = "CURATED_ACCOUNT_SERVING_TABLE_NAME";
    protected static final String CURATED_ACCOUNT_STATS_TABLE_NAME = "CURATED_ACCOUNT_STATS_TABLE_NAME";
    protected static final String CURATED_CONTACT_SERVING_TABLE_NAME = "CURATED_CONTACT_SERVING_TABLE_NAME";
    protected static final String CURATED_CONTACT_STATS_TABLE_NAME = "CURATED_CONTACT_STATS_TABLE_NAME";
    protected static final String REMATCHED_ACCOUNT_TABLE_NAME = "REMATCHED_ACCOUNT_TABLE_NAME";
    protected static final String ENRICHED_ACCOUNT_DIFF_TABLE_NAME = "ENRICHED_ACCOUNT_DIFF_TABLE_NAME";

    public static final String EXISTING_RECORDS = "EXISTING_RECORDS";
    public static final String UPDATED_RECORDS = "UPDATED_RECORDS";
    public static final String NEW_RECORDS = "NEW_RECORDS";
    public static final String DELETED_RECORDS = "DELETED_RECORDS";
    public static final String TOTAL_RECORDS = "TOTAL_RECORDS";
    public static final String FINAL_RECORDS = "FINAL_RECORDS";
    public static final String MERGED_PRODUCT_ID = "MERGED_PRODUCT_ID";
    public static final String MERGED_PRODUCT_BUNDLE = "MERGED_PRODUCT_BUNDLE";
    public static final String MERGED_PRODUCT_HIERARCHY = "MERGED_PRODUCT_HIERARCHY";
    public static final String MERGED_FILE_NAME = "MERGED_FILE_NAME";
    public static final String ORPHAN_COUNT = "ORPHAN_COUNT";
    protected static final String ENTITY_PUBLISH_STATS = "ENTITY_PUBLISH_STATS";

    protected static final String CUSTOM_EVENT_IMPORT = "CUSTOM_EVENT_IMPORT";
    protected static final String CUSTOM_EVENT_MATCH_ATTRIBUTES = "CUSTOM_EVENT_MATCH_ATTRIBUTES";
    protected static final String CUSTOM_EVENT_MATCH_ACCOUNT = "CUSTOM_EVENT_MATCH_ACCOUNT";
    protected static final String CUSTOM_EVENT_MATCH_ACCOUNT_ID = "CUSTOM_EVENT_MATCH_ACCOUNT_ID";
    protected static final String CUSTOM_EVENT_MATCH_WITHOUT_ACCOUNT_ID = "CUSTOM_EVENT_MATCH_WITHOUT_ACCOUNT_ID";
    protected static final String DATA_STREAM = "DataStream";

    protected static final String INPUT_SKIPPED_ATTRIBUTES_KEY = "INPUT_SKIPPED_ATTRIBUTES_KEY";

    public static final String PROCESS_ANALYTICS_DECISIONS_KEY = "PROCESS_ANALYTICS_DECISIONS_KEY";
    public static final String PROCESS_ANALYTICS_WARNING_KEY = "PROCESS_ANALYTICS_WARNING_KEY";
    public static final String CHOREOGRAPHER_CONTEXT_KEY = "CHOREOGRAPHER_CONTEXT_KEY";

    public static final String DATAQUOTA_LIMIT = "DATAQUOTA_LIMIT";
    public static final String ATTRIBUTE_QUOTA_LIMIT = "ATTRIBUTE_QUOTA_LIMIT";

    public static final String REMATCH_TABLE_NAMES = "REMATCH_TABLE_NAMES";
    public static final String DELETED_TABLE_NAMES = "DELETED_TABLE_NAMES";
    public static final String ENTITY_MATCH_ENABLED = "ENTITY_MATCH_ENABLED";
    public static final String FULL_REMATCH_PA = "FULL_REMATCH_PA";
    /*-
     * in rematch, staging version should persist across PAs, serving should not
     */
    public static final String ENTITY_MATCH_REMATCH_STAGING_VERSION = "ENTITY_MATCH_REMATCH_STAGING_VERSION";
    public static final String ENTITY_MATCH_REMATCH_SERVING_VERSION = "ENTITY_MATCH_REMATCH_SERVING_VERSION";

    public static final String ACCOUNT_LEGACY_DELTE_BYUOLOAD_ACTIONS = "ACCOUNT_LEGACY_DELTE_BYUOLOAD_ACTIONS";
    public static final String CONTACT_LEGACY_DELTE_BYUOLOAD_ACTIONS = "CONTACT_LEGACY_DELTE_BYUOLOAD_ACTIONS";
    public static final String TRANSACTION_LEGACY_DELTE_BYUOLOAD_ACTIONS = "TRANSACTION_LEGACY_DELTE_BYUOLOAD_ACTIONS";
    public static final String LEGACY_DELETE_BYDATERANGE_ACTIONS = "LEGACY_DELETE_BYDATERANGE_ACTIONS";
    public static final String LEGACY_DELETE_MERGE_TABLENAMES = "LEGACY_DELETE_MERGE_TABLENAMES";

    public static final String TRACING_CONTEXT = "TRACING_CONTEXT";

    public static final String REGISTERED_TABLE_NAMES = "REGISTERED_TABLE_NAMES";

    public static final String TIMELINE_EXPORT_ACCOUNTLIST = "TIMELINE_EXPORT_ACCOUNTLIST";
    public static final String TIMELINE_EXPORT_TABLES = "TIMELINE_EXPORT_TABLES";
    public static final String TIMELINE_EXPORT_FILES = "TIMELINE_EXPORT_FILES";
    public static final String IS_SSVI_TENANT = "IS_SSVI_TENANT";
    public static final String IS_CDL_TENANT = "IS_CDL_TENANT";
    public static final String SSVI_WEBVISIT_RAW_TABLE = "SSVI_WEBVISIT_RAW_TABLE";

    public static final String TABLEROLES_GOING_TO_ES = "TABLEROLES_GOING_TO_ES";

    // tables to be carried over in restarted PA
    protected static final Set<String> TABLE_NAMES_FOR_PA_RETRY = Sets.newHashSet( //
            ENTITY_MATCH_ACCOUNT_TARGETTABLE, //
            ENTITY_MATCH_CONTACT_TARGETTABLE, //
            ENTITY_MATCH_CONTACT_ACCOUNT_TARGETTABLE, //
            ENTITY_MATCH_TXN_TARGETTABLE, //
            ENTITY_MATCH_TXN_ACCOUNT_TARGETTABLE, //
            ACCOUNT_DIFF_TABLE_NAME, //
            ACCOUNT_CHANGELIST_TABLE_NAME, //
            ACCOUNT_REPORT_CHANGELIST_TABLE_NAME, //
            SYSTEM_ACCOUNT_MASTER_TABLE_NAME, //
            ACCOUNT_MASTER_TABLE_NAME, //
            FULL_ACCOUNT_TABLE_NAME, //
            LATTICE_ACCOUNT_TABLE_NAME, //
            LATTICE_ACCOUNT_PROFILE_TABLE_NAME, //
            LATTICE_ACCOUNT_CHANGELIST_TABLE_NAME, //
            ACCOUNT_EXPORT_TABLE_NAME, //
            ACCOUNT_FEATURE_TABLE_NAME, //
            ACCOUNT_PROFILE_TABLE_NAME, //
            ACCOUNT_SERVING_TABLE_NAME, //
            ACCOUNT_STATS_TABLE_NAME, //
            ACCOUNT_STATS_DIFF_TABLE_NAME, //
            FULL_ACCOUNT_STATS_TABLE_NAME, //
            ACCOUNT_LOOKUP_TABLE_NAME, //
            REMATCHED_ACCOUNT_TABLE_NAME, //
            ENRICHED_ACCOUNT_DIFF_TABLE_NAME, //
            CONTACT_CHANGELIST_TABLE_NAME, //
            CONTACT_SERVING_TABLE_NAME, //
            CONTACT_PROFILE_TABLE_NAME, //
            CONTACT_STATS_TABLE_NAME, //
            CONTACT_STATS_DIFF_TABLE_NAME, //
            ROLLUP_PRODUCT_TABLE, //
            DAILY_TRXN_TABLE_NAME, //
            AGG_DAILY_TRXN_TABLE_NAME, //
            AGG_PERIOD_TRXN_TABLE_NAME, //
            PH_SERVING_TABLE_NAME, //
            PH_DEPIVOTED_TABLE_NAME, //
            PH_PROFILE_TABLE_NAME, //
            PH_STATS_TABLE_NAME, //
            JOURNEY_STAGE_TABLE_NAME, //
            ACTIVITY_ALERT_MASTER_TABLE_NAME, //
            ACTIVITY_ALERT_DIFF_TABLE_NAME, //
            CURATED_ACCOUNT_SERVING_TABLE_NAME, //
            CURATED_ACCOUNT_STATS_TABLE_NAME, //
            CURATED_CONTACT_SERVING_TABLE_NAME, //
            CURATED_CONTACT_STATS_TABLE_NAME, //
            SSVI_WEBVISIT_RAW_TABLE //
    );
    protected static final Set<String> TABLE_NAME_LISTS_FOR_PA_RETRY = Sets.newHashSet(PERIOD_TRXN_TABLE_NAME);

    protected static final Set<String> TABLE_NAME_MAPS_FOR_PA_RETRY = Sets.newHashSet( //
            CATALOG_TABLE_NAME, //
            ENTITY_MATCH_STREAM_TARGETTABLE, //
            ENTITY_MATCH_STREAM_ACCOUNT_TARGETTABLE, //
            ENTITY_MATCH_STREAM_CONTACT_TARGETTABLE, //
            RAW_ACTIVITY_STREAM_TABLE_NAME, //
            AGG_DAILY_ACTIVITY_STREAM_TABLE_NAME, //
            DAILY_ACTIVITY_STREAM_DELTA_TABLE_NAME, //
            METRICS_GROUP_TABLE_NAME, //
            MERGED_METRICS_GROUP_TABLE_NAME, //
            PERIOD_STORE_TABLE_NAME, //
            TIMELINE_MASTER_TABLE_NAME, //
            TIMELINE_DIFF_TABLE_NAME, //
            LAST_ACTIVITY_DATE_TABLE_NAME, //
            Raw_TXN_STREAMS, //
            DAILY_TXN_STREAMS, //
            PERIOD_TXN_STREAMS, //
            PERIOD_TRXN_TABLE_NAMES_BY_PERIOD_NAME //
    );

    protected static final Set<String> REMATCH_TABLE_NAMES_FOR_PA_RETRY = Sets.newHashSet( //
            REMATCH_TABLE_NAMES, //
            DELETED_TABLE_NAMES);

    // extra context keys to be carried over in restarted PA, beyond table names
    // above
    protected static final Set<String> EXTRA_KEYS_FOR_PA_RETRY = Sets.newHashSet( //
            PA_TIMESTAMP, //
            ACCOUNT_RE_PROFILE_ATTRS, //
            LATTICE_ACCOUNT_RE_PROFILE_ATTRS, //
            CONTACT_RE_PROFILE_ATTRS, //
            STATS_UPDATED, //
            ACCOUNT_STATS_UPDATED, //
            CONTACT_STATS_UPDATED, //
            REBUILD_LATTICE_ACCOUNT, //
            ENTITY_MATCH_COMPLETED, //
            NEW_ENTITY_MATCH_ENVS, //
            FULL_REMATCH_PA, //
            ENTITY_MATCH_REMATCH_STAGING_VERSION, //
            NEW_RECORD_CUT_OFF_TIME, //
            CONSOLIDATE_INPUT_TEMPLATES, //
            PROCESS_ACCOUNT_STATS_MERGE, //
            ACTIVITY_STREAMS_RELINK, //
            ACTIVITY_METRICS_CATEGORICAL_ATTR, //
            ACTIVITY_METRICS_CATEGORIES, //
            ACTIVITY_ALERT_GENERATED, //
            ACTIVITY_ALERT_PUBLISHED, //
            RETAIN_PRODUCT_TYPE,
            IS_SSVI_TENANT,
            IS_CDL_TENANT);

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected YarnClient yarnClient;

    @Autowired
    protected ModelProxy modelProxy;

    @Autowired
    protected JobProxy jobProxy;

    @Autowired
    protected WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    protected EmailProxy emailProxy;

    @Autowired
    protected LatticeInsightsInternalProxy latticeInsightsInternalProxy;

    @Autowired
    protected WorkflowReportService workflowReportService;

    @Autowired
    protected SourceFileProxy sourceFileProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    protected DataUnitProxy dataUnitProxy;

    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor();
    protected List<ClientHttpRequestInterceptor> addMagicAuthHeaders = Arrays
            .asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader });
    protected RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    @Override
    public boolean setup() {
        boolean result = super.setup();
        restTemplate.setInterceptors(addMagicAuthHeaders);
        return result;
    }

    protected void waitForAppId(String appId) {
        log.info(String.format("Waiting for appId: %s", appId));

        JobStatus status;
        int maxTries = 17280; // Wait maximum 24 hours
        int i = 0;
        do {
            status = jobProxy.getJobStatus(appId);
            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                // Do nothing for InterruptedException
                log.info(e.getMessage());
            }
            i++;

            if (i == maxTries) {
                break;
            }
        } while (!YarnUtils.TERMINAL_STATUS.contains(status.getStatus()));
        if (status.getStatus() != FinalApplicationStatus.SUCCEEDED) {
            throw new LedpException(WorkflowUtils.getWorkFlowErrorCode(status.getDiagnostics()),
                    new String[] { appId, status.getErrorReport() });
        }
    }

    protected String getHdfsDir(String path) {
        String[] tokens = StringUtils.split(path, "/");
        String[] newTokens;

        if (path.endsWith("avro")) {
            newTokens = new String[tokens.length - 1];
        } else {
            newTokens = new String[tokens.length];
        }
        System.arraycopy(tokens, 0, newTokens, 0, newTokens.length);
        return "/" + StringUtils.join(newTokens, "/");
    }

    @SuppressWarnings("unchecked")
    protected void saveOutputValue(String key, String val) {
        putOutputValue(key, val);
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(jobId);
        workflowJob.setOutputContext(getObjectFromContext(WorkflowContextConstants.OUTPUTS, Map.class));
        workflowJobEntityMgr.updateWorkflowJob(workflowJob);
    }

    protected void saveReport(Map<String, String> map) {
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(jobId);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            workflowJob.setReportName(entry.getKey(), entry.getValue());
        }
        workflowJobEntityMgr.updateReport(workflowJob);
    }

    public void setWorkflowJobEntityMgr(WorkflowJobEntityMgr workflowJobEntityMgr) {
        this.workflowJobEntityMgr = workflowJobEntityMgr;
    }

    protected SourceFile retrieveSourceFile(CustomerSpace space, String name) {
        if (name == null) {
            return null;
        }
        return sourceFileProxy.findByName(space.toString(), name);
    }

    protected void registerReport(CustomerSpace customerSpace, Report report) {
        @SuppressWarnings("unchecked")
        Map<String, String> map = getObjectFromContext(WorkflowContextConstants.REPORTS, Map.class);

        if (map == null) {
            map = new HashMap<>();
        }

        map.put(report.getPurpose().getKey(), report.getName());

        saveReport(map);
        putObjectInContext(WorkflowContextConstants.REPORTS, map);
        RetryTemplate template = RetryUtils.getExponentialBackoffRetryTemplate(3, 5000L, 2.0, null);
        template.execute(context -> {
            if (context.getRetryCount() >= 1) {
                log.warn("Last registering report for tenant {} failed. Retrying for {} times.", customerSpace,
                        context.getRetryCount());
            }
            workflowReportService.createOrUpdateReport(customerSpace.toString(), report);
            return null;
        });
    }

    protected Report retrieveReport(CustomerSpace space, ReportPurpose purpose) {
        @SuppressWarnings("unchecked")
        Map<String, String> map = getObjectFromContext(WorkflowContextConstants.REPORTS, Map.class);

        if (map == null) {
            return null;
        }

        String name = map.get(purpose.getKey());
        if (name == null) {
            return null;
        }
        return workflowReportService.findReportByName(space.toString(), name);
    }

    protected Report createReport(String json, ReportPurpose purpose, String name) {
        Report report = new Report();
        KeyValue kv = new KeyValue();
        kv.setPayload(json);
        report.setJson(kv);
        report.setPurpose(purpose);
        report.setName(name);
        return report;
    }

    protected Map<String, String> retrieveModelIds(Map<String, ModelSummary> eventToModelSummary) {
        Map<String, String> eventToModelId = new HashMap<>();
        for (String event : eventToModelSummary.keySet()) {
            eventToModelId.put(event, eventToModelSummary.get(event).getId());
        }
        return eventToModelId;
    }

    protected void skipEmbeddedWorkflow(String parentNamespace, String workflowName,
            Class<? extends WorkflowConfiguration> workflowClass) {
        Map<String, BaseStepConfiguration> stepConfigMap = getStepConfigMapInWorkflow(parentNamespace, workflowName,
                workflowClass);
        stepConfigMap.forEach((name, step) -> {
            step.setSkipStep(true);
            putObjectInContext(name, step);
            log.info("Set step " + name + " to be skipped.");
        });
    }

    protected void skipEmbeddedWorkflowSteps(String parentNamespace, String workflowName,
            Class<? extends WorkflowConfiguration> workflowClass, List<String> stepNames) {
        Map<String, BaseStepConfiguration> stepConfigMap = getStepConfigMapInWorkflow(parentNamespace, workflowName,
                workflowClass);
        String newParentNamespace = StringUtils.isEmpty(parentNamespace) ? "" : parentNamespace + ".";
        stepNames.forEach(s -> {
            String key = newParentNamespace + workflowName + "." + s;
            BaseStepConfiguration step = stepConfigMap.get(key);
            step.setSkipStep(true);
            putObjectInContext(key, step);
        });
    }

    protected void enableEmbeddedWorkflow(String parentNamespace, String workflowName,
            Class<? extends WorkflowConfiguration> workflowClass) {
        Map<String, BaseStepConfiguration> stepConfigMap = getStepConfigMapInWorkflow(parentNamespace, workflowName,
                workflowClass);
        stepConfigMap.forEach((name, step) -> {
            if (step.isSkipStep()) {
                step.setSkipStep(false);
                putObjectInContext(name, step);
                log.info("Set step " + name + " to be enabled.");
            }
        });
    }

    public Map<String, BaseStepConfiguration> getStepConfigMapInWorkflow(String parentNamespace, String workflowName,
            Class<? extends WorkflowConfiguration> workflowConfigClass) {
        String newParentNamespace = StringUtils.isEmpty(parentNamespace) ? "" : parentNamespace + ".";
        String ns = newParentNamespace + workflowConfigClass.getSimpleName();
        WorkflowConfiguration workflowConfig = getObjectFromContext(ns, workflowConfigClass);
        if (workflowConfig == null) {
            log.warn("There is no workflow configuration of class " + workflowConfigClass.getSimpleName()
                    + " in context.");
            try {
                workflowConfig = WorkflowConfigurationUtils.getDefaultWorkflowConfiguration(workflowConfigClass);
                if (StringUtils.isNotEmpty(workflowName)) {
                    workflowConfig.setWorkflowName(workflowName);
                }
            } catch (Exception e) {
                throw new RuntimeException(String.format("Can't instantiate workflow configuration %s",
                        workflowConfigClass.getSimpleName()), e);
            }
        }
        Map<String, String> registry = WorkflowUtils.getFlattenedConfig(workflowConfig);
        return registry.entrySet().stream().collect(Collectors.toMap(e -> newParentNamespace + e.getKey(), e -> {
            String namespace = newParentNamespace + e.getKey();
            BaseStepConfiguration step = getObjectFromContext(namespace, BaseStepConfiguration.class);
            if (step == null) {
                step = getConfigurationFromJobParameters(namespace);
                if (step == null) {
                    step = JsonUtils.deserialize(e.getValue(), BaseStepConfiguration.class);
                }
            }
            return step;
        }));
    }

    /**
     * Retrieve table summary for all tables in target context keys.
     *
     * @param customer
     *            customerSpace
     * @param tableNameStrCtxKeys
     *            list of context keys that contain a single table as string
     * @param tableNameListCtxKeys
     *            list of context keys that contain a list of tables as serialized
     *            list of string
     * @return list of tables, null will be inserted if the corresponding table does
     *         not exist
     */
    protected List<Table> getTableSummariesFromCtxKeys(String customer, List<String> tableNameStrCtxKeys,
            List<String> tableNameListCtxKeys) {
        List<Table> tables = new ArrayList<>(getTableSummariesFromCtxKeys(customer, tableNameStrCtxKeys));
        if (CollectionUtils.isNotEmpty(tableNameListCtxKeys)) {
            tables.addAll(tableNameListCtxKeys.stream() //
                    .map(key -> getTableSummariesFromListKey(customer, key)) //
                    .filter(Objects::nonNull) //
                    .flatMap(List::stream) //
                    .collect(Collectors.toList()));
        }
        return tables;
    }

    protected Map<String, Table> getTablesFromMapCtxKey(String customer, String tableMapCtxKey) {
        if (!hasKeyInContext(tableMapCtxKey)) {
            return Collections.emptyMap();
        }

        Map<String, String> tableNames = getMapObjectFromContext(tableMapCtxKey, String.class, String.class);
        return tableNames.entrySet() //
                .stream() //
                .map(entry -> Pair.of(entry.getKey(), metadataProxy.getTable(customer, entry.getValue())))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    protected List<Table> getTableSummariesFromCtxKeys(String customer, List<String> tableNameCtxKeys) {
        if (CollectionUtils.isEmpty(tableNameCtxKeys)) {
            return Collections.emptyList();
        } else {
            return tableNameCtxKeys.stream() //
                    .map(k -> getTableSummaryFromKey(customer, k)).collect(Collectors.toList());
        }
    }

    protected List<Table> getTableSummariesFromListKey(String customer, String tableNameListCtxKey) {
        List<String> tableNames = getListObjectFromContext(tableNameListCtxKey, String.class);
        if (CollectionUtils.isNotEmpty(tableNames)) {
            return tableNames.stream().map(name -> getTableSummary(customer, name)).collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    protected List<Table> getTableSummaries(String customer, List<String> tableNames) {
        if (CollectionUtils.isNotEmpty(tableNames)) {
            return tableNames.stream().map(name -> getTableSummary(customer, name)).collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    protected boolean hasTableInCtxKey(String customer, String tableNameCtxKey) {
        Table table = getTableSummaryFromKey(customer, tableNameCtxKey);
        return table != null;
    }

    protected Table getTableSummaryFromKey(String customer, String tableNameCtxKey) {
        String tableName = getStringValueFromContext(tableNameCtxKey);
        return getTableSummary(customer, tableName);
    }

    protected Table getTableSummary(String customer, String tableName) {
        if (StringUtils.isNotBlank(tableName)) {
            RetryTemplate retry = RetryUtils.getRetryTemplate(3);
            return retry.execute(ctx -> metadataProxy.getTableSummary(customer, tableName));
        }
        return null;
    }

    /*-
     * temp flag to track whether a tenant's transaction store has been migrated off CustomerAccountId
     * TODO remove after all tenants are rebuilt
     */
    protected boolean isTransactionRebuilt() {
        DataCollectionStatus status = getObjectFromContext(BaseWorkflowStep.CDL_COLLECTION_STATUS,
                DataCollectionStatus.class);
        return status != null && BooleanUtils.isTrue(status.getTransactionRebuilt());
    }

    protected Set<String> getTableNamesForPaRetry() {
        Stream<String> strTableStream = TABLE_NAMES_FOR_PA_RETRY.stream().map(this::getStringValueFromContext);
        Stream<String> listTableStream = TABLE_NAME_LISTS_FOR_PA_RETRY.stream() //
                .flatMap(key -> {
                    List<String> tableNames = getListObjectFromContext(key, String.class);
                    if (CollectionUtils.isEmpty(tableNames)) {
                        return Stream.empty();
                    } else {
                        return tableNames.stream();
                    }
                });
        Stream<String> mapTableStream = TABLE_NAME_MAPS_FOR_PA_RETRY.stream() //
                .filter(this::hasKeyInContext) //
                .flatMap(key -> {
                    Map<Object, String> tableNames = getMapObjectFromContext(key, Object.class, String.class);
                    if (MapUtils.isEmpty(tableNames)) {
                        return Stream.empty();
                    } else {
                        return tableNames.values().stream();
                    }
                });
        Stream<String> rematchTableStream = REMATCH_TABLE_NAMES_FOR_PA_RETRY.stream() //
                .filter(this::hasKeyInContext) //
                .flatMap(key -> {
                    Map<String, List<String>> tableNames = getTypedObjectFromContext(key,
                            new TypeReference<Map<String, List<String>>>() {
                            });
                    if (MapUtils.isEmpty(tableNames)) {
                        return Stream.empty();
                    } else {
                        return tableNames.values().stream().flatMap(list -> list.stream());
                    }
                });
        return Stream
                .concat(Stream.concat(Stream.concat(strTableStream, listTableStream), mapTableStream),
                        rematchTableStream) //
                .filter(StringUtils::isNotBlank) //
                .collect(Collectors.toSet());
    }

    protected Set<String> getRenewableCtxKeys() {
        Set<String> renewableKeys = new HashSet<>(TABLE_NAMES_FOR_PA_RETRY);
        renewableKeys.addAll(TABLE_NAME_LISTS_FOR_PA_RETRY);
        renewableKeys.addAll(TABLE_NAME_MAPS_FOR_PA_RETRY);
        renewableKeys.addAll(REMATCH_TABLE_NAMES_FOR_PA_RETRY);
        renewableKeys.addAll(EXTRA_KEYS_FOR_PA_RETRY);
        return renewableKeys;
    }

    protected void registerTable(String tableName) {
        Set<String> registeredTableNames = getSetObjectFromContext(REGISTERED_TABLE_NAMES, String.class);
        if (CollectionUtils.isEmpty(registeredTableNames)) {
            registeredTableNames = new HashSet<>();
        }
        registeredTableNames.add(tableName);
        putObjectInContext(REGISTERED_TABLE_NAMES, registeredTableNames);
    }

    protected String getApplicationId() {
        String applicationId = null;
        if (jobId != null) {
            WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobId);
            if (job != null && job.getApplicationId() != null) {
                applicationId = ConverterUtils.toApplicationId(job.getApplicationId()).toString();
            }
        }
        return applicationId;
    }

    protected boolean isResettingEntity(@NotNull BusinessEntity entity) {
        Set<BusinessEntity> entitySet = getSetObjectFromContext(RESET_ENTITIES, BusinessEntity.class);
        return CollectionUtils.emptyIfNull(entitySet).contains(entity);
    }

    protected List<String> getEntityTemplates(@NotNull BusinessEntity entity) {
        Map<BusinessEntity, List> templates = getMapObjectFromContext(CONSOLIDATE_TEMPLATES_IN_ORDER,
                BusinessEntity.class, List.class);
        List<String> templateNames;
        if (MapUtils.isNotEmpty(templates) && templates.containsKey(entity)) {
            templateNames = JsonUtils.convertList(templates.get(entity), String.class);
        } else {
            templateNames = Collections.emptyList();
        }

        log.info("{} templates = {}", entity.name(), templateNames);
        return templateNames;
    }

    protected long getCurrentTimestamp() {
        String evaluationDateStr = getStringValueFromContext(CDL_EVALUATION_DATE);
        if (StringUtils.isNotBlank(evaluationDateStr)) {
            long currTime = LocalDate
                    .parse(evaluationDateStr, DateTimeFormatter.ofPattern(DateTimeUtils.DATE_ONLY_FORMAT_STRING)) //
                    .atStartOfDay(ZoneId.of("UTC")) // start of date in UTC
                    .toInstant() //
                    .toEpochMilli();
            log.info("Found evaluation date {}, use end of date as current time. Timestamp = {}", evaluationDateStr,
                    currTime);
            return currTime;
        } else {
            Long paTime = getLongValueFromContext(PA_TIMESTAMP);
            Preconditions.checkNotNull(paTime, "pa timestamp should be set in context");
            log.info("No evaluation date str found in context, use pa timestamp = {}", paTime);
            return paTime;
        }
    }

    protected Long getLastEvaluationTime(@NotNull TableRoleInCollection role) {
        Preconditions.checkNotNull(role, "Table role should not be null");
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        return MapUtils.emptyIfNull(dcStatus.getEvaluationDateMap()).get(role.name());
    }

    protected void setLastEvaluationTime(@NotNull Long currentTimestamp, @NotNull TableRoleInCollection role) {
        Preconditions.checkNotNull(currentTimestamp, "Current time should be set already");
        Preconditions.checkNotNull(role, "Table role should not be null");
        DataCollectionStatus dcStatus = getObjectFromContext(CDL_COLLECTION_STATUS, DataCollectionStatus.class);
        if (dcStatus.getEvaluationDateMap() == null) {
            dcStatus.setEvaluationDateMap(new HashMap<>());
        }
        dcStatus.getEvaluationDateMap().put(role.name(), currentTimestamp);
        log.info("Set evaluation date for {} to {}", role.name(), currentTimestamp);
        putObjectInContext(CDL_COLLECTION_STATUS, dcStatus);
    }
}

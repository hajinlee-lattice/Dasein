package com.latticeengines.workflow.exposed.build;

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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.util.WorkflowConfigurationUtils;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.KeyValue;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.dataplatform.JobProxy;
import com.latticeengines.proxy.exposed.dataplatform.ModelProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.util.WorkflowUtils;

import avro.shaded.com.google.common.collect.Sets;

public abstract class BaseWorkflowStep<T extends BaseStepConfiguration> extends AbstractStep<T> {

    protected static final Logger log = LoggerFactory.getLogger(BaseWorkflowStep.class);

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
    protected static final String SCORING_MODEL_TYPE = "SCORING_MODEL_TYPE";
    protected static final String SCORING_SOURCE_DIR = "SCORING_SOURCE_DIR";
    protected static final String SCORING_UNIQUEKEY_COLUMN = "SCORING_UNIQUEKEY_COLUMN";
    protected static final String ATTR_LEVEL_TYPE = "ATTR_LEVEL_TYPE";
    protected static final String IMPORT_DATA_APPLICATION_ID = "IMPORT_DATA_APPLICATION_ID";
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

    // CDL
    public static final String CONSOLIDATE_INPUT_IMPORTS = "CONSOLIDATE_INPUT_IMPORTS";
    public static final String CDL_ACTIVE_VERSION = "CDL_ACTIVE_VERSION";
    public static final String CUSTOMER_SPACE = "CUSTOMER_SPACE";
    public static final String TABLES_GOING_TO_DYNAMO = "TABLES_GOING_TO_DYNAMO";
    public static final String TABLES_GOING_TO_REDSHIFT = "TABLES_GOING_TO_REDSHIFT";
    public static final String ENTITIES_WITH_SCHEMA_CHANGE = "ENTITIES_WITH_SCHEMA_CHANGE";
    public static final String RATING_MODELS = "RATING_MODELS";
    public static final String ACTION_IMPACTED_SEGMENTS = "ACTION_IMPACTED_SEGMENTS";
    public static final String ACTION_IMPACTED_ENGINES = "ACTION_IMPACTED_ENGINES";
    public static final String RESCORE_ALL_RATINGS = "RESCORE_ALL_RATINGS";
    public static final String CURRENT_RATING_ITERATION = "CURRENT_RATING_ITERATION";
    public static final String INACTIVE_ENGINE_ATTRIBUTES = "INACTIVE_ENGINE_ATTRIBUTES";
    public static final String INACTIVE_ENGINES = "INACTIVE_ENGINES";
    public static final String ITERATION_RATING_MODELS = "ITERATION_RATING_MODELS";
    public static final String RATING_MODELS_BY_ITERATION = "RATING_MODELS_BY_ITERATION";
    protected static final String IMPACTED_ENTITIES = "IMPACTED_ENTITIES";
    protected static final String ITERATION_INACTIVE_ENGINES = "ITERATION_INACTIVE_ENGINES";
    protected static final String INACTIVE_RATINGS_TABLE_NAME = "INACTIVE_RATINGS_TABLE_NAME";
    protected static final String RATING_ENGINE_ID_TO_ACTIVATE = "RATING_ENGINE_ID_TO_ACTIVATE";
    protected static final String RULE_RAW_RATING_TABLE_NAME = "RULE_RAW_RATING_TABLE_NAME";
    protected static final String AI_RAW_RATING_TABLE_NAME = "AI_RAW_RATING_TABLE_NAME";
    protected static final String PIVOTED_RATINGS_TABLE_NAME = "PIVOTED_RATINGS_TABLE_NAME";
    protected static final String CDL_INACTIVE_VERSION = "CDL_INACTIVE_VERSION";
    protected static final String CDL_EVALUATION_DATE = "CDL_EVALUATION_DATE";
    protected static final String CDL_COLLECTION_STATUS = "CDL_COLLECTION_STATUS";
    public static final String SYSTEM_ACTION_IDS = "SYSTEM_ACTION_IDS";
    protected static final String PA_TIMESTAMP = "PA_TIMESTAMP";
    protected static final String NEW_RECORD_CUT_OFF_TIME = "NEW_RECORD_CUT_OFF_TIME";
    public static final String PA_SKIP_ENTITIES = "PA_SKIP_ENTITIES";
    protected static final String CLEANUP_TIMESTAMP = "CLEANUP_TIMESTAMP";
    protected static final String STATS_TABLE_NAMES = "STATS_TABLE_NAMES";
    protected static final String TEMPORARY_CDL_TABLES = "TEMPORARY_CDL_TABLES";
    protected static final String ENTITY_DIFF_TABLES = "ENTITY_DIFF_TABLES";
    protected static final String PROCESSED_DIFF_TABLES = "PROCESSED_DIFF_TABLES";
    protected static final String RESET_ENTITIES = "RESET_ENTITIES";
    protected static final String EVALUATION_PERIOD = "EVALUATION_PERIOD";
    protected static final String HAS_CROSS_SELL_MODEL = "HAS_CROSS_SELL_MODEL";
    protected static final String RATING_LIFTS = "RATING_LIFTS";
    protected static final String BUCKETED_SCORE_SUMMARIES = "BUCKETED_SCORE_SUMMARIES";
    protected static final String BUCKET_METADATA_MAP = "BUCKET_METADATA_MAP";
    protected static final String MODEL_GUID_ENGINE_ID_MAP = "MODEL_GUID_ENGINE_ID_MAP";
    protected static final String BUCKETED_SCORE_SUMMARIES_AGG = "BUCKETED_SCORE_SUMMARIES_AGG";
    protected static final String BUCKET_METADATA_MAP_AGG = "BUCKET_METADATA_MAP_AGG";
    protected static final String MODEL_GUID_ENGINE_ID_MAP_AGG = "MODEL_GUID_ENGINE_ID_MAP_AGG";
    protected static final String SKIP_PUBLISH_PA_TO_S3 = "SKIP_PUBLISH_PA_TO_S3";
    protected static final String ATLAS_EXPORT_DATA_UNIT = "ATLAS_EXPORT_DATA_UNIT";
    protected static final String ATLAS_EXPORT_DELETE_PATH = "ATLAS_EXPORT_DELETE_PATH";
    protected static final String PRIMARY_IMPORT_SYSTEM = "PRIMARY_IMPORT_SYSTEM";
    protected static final String ADDED_ACCOUNTS_DELTA_TABLE = "ADDED_ACCOUNTS_DELTA_TABLE";
    protected static final String REMOVED_ACCOUNTS_DELTA_TABLE = "REMOVED_ACCOUNTS_DELTA_TABLE";
    protected static final String ADDED_CONTACTS_DELTA_TABLE = "ADDED_CONTACTS_DELTA_TABLE";
    protected static final String REMOVED_CONTACTS_DELTA_TABLE = "REMOVED_CONTACTS_DELTA_TABLE";
    protected static final String FULL_ACCOUNTS_UNIVERSE = "FULL_ACCOUNTS_UNIVERSE";
    protected static final String FULL_CONTACTS_UNIVERSE = "FULL_CONTACTS_UNIVERSE";
    protected static final String RECOMMENDATION_ACCOUNT_DISPLAY_NAMES = "RECOMMENDATION_ACCOUNT_DISPLAY_NAMES";
    protected static final String RECOMMENDATION_CONTACT_DISPLAY_NAMES = "RECOMMENDATION_CONTACT_DISPLAY_NAMES";

    // intermediate results for skippable steps
    protected static final String NEW_ENTITY_MATCH_ENVS = "NEW_ENTITY_MATCH_ENVS";
    protected static final String ENTITY_MATCH_COMPLETED = "ENTITY_MATCH_COMPLETED";
    protected static final String ENTITY_MATCH_ACCOUNT_TARGETTABLE = "ENTITY_MATCH_ACCOUNT_TARGETTABLE";
    protected static final String ENTITY_MATCH_CONTACT_TARGETTABLE = "ENTITY_MATCH_CONTACT_TARGETTABLE";
    protected static final String ENTITY_MATCH_TXN_TARGETTABLE = "ENTITY_MATCH_TXN_TARGETTABLE";
    public static final String ENTITY_MATCH_CONTACT_ACCOUNT_TARGETTABLE = "ENTITY_MATCH_CONTACT_ACCOUNT_TARGETTABLE";
    public static final String ENTITY_MATCH_TXN_ACCOUNT_TARGETTABLE = "ENTITY_MATCH_TXN_ACCOUNT_TARGETTABLE";
    protected static final String ACCOUNT_DIFF_TABLE_NAME = "ACCOUNT_DIFF_TABLE_NAME";
    protected static final String ACCOUNT_MASTER_TABLE_NAME = "ACCOUNT_MASTER_TABLE_NAME";
    protected static final String FULL_ACCOUNT_TABLE_NAME = "FULL_ACCOUNT_TABLE_NAME";
    protected static final String ACCOUNT_FEATURE_TABLE_NAME = "ACCOUNT_FEATURE_TABLE_NAME";
    protected static final String ACCOUNT_EXPORT_TABLE_NAME = "ACCOUNT_EXPORT_TABLE_NAME";
    protected static final String ACCOUNT_PROFILE_TABLE_NAME = "ACCOUNT_PROFILE_TABLE_NAME";
    protected static final String ACCOUNT_SERVING_TABLE_NAME = "ACCOUNT_SERVING_TABLE_NAME";
    protected static final String ACCOUNT_STATS_TABLE_NAME = "ACCOUNT_STATS_TABLE_NAME";
    protected static final String CONTACT_SERVING_TABLE_NAME = "CONTACT_SERVING_TABLE_NAME";
    protected static final String CONTACT_PROFILE_TABLE_NAME = "CONTACT_PROFILE_TABLE_NAME";
    protected static final String CONTACT_STATS_TABLE_NAME = "CONTACT_STATS_TABLE_NAME";
    protected static final String DAILY_TRXN_TABLE_NAME = "DAILY_TRXN_TABLE_NAME";
    protected static final String AGG_DAILY_TRXN_TABLE_NAME = "AGG_DAILY_TRXN_TABLE_NAME";
    protected static final String PERIOD_TRXN_TABLE_NAME = "PERIOD_TRXN_TABLE_NAME";
    protected static final String AGG_PERIOD_TRXN_TABLE_NAME = "AGG_PERIOD_TRXN_TABLE_NAME";
    protected static final String PH_SERVING_TABLE_NAME = "PH_SERVING_TABLE_NAME";
    protected static final String PH_PROFILE_TABLE_NAME = "PH_PROFILE_TABLE_NAME";
    protected static final String PH_STATS_TABLE_NAME = "PH_STATS_TABLE_NAME";
    protected static final String PH_DEPIVOTED_TABLE_NAME = "PH_DEPIVOTED_TABLE_NAME";
    protected static final String CURATED_ACCOUNT_SERVING_TABLE_NAME = "CURATED_ACCOUNT_SERVING_TABLE_NAME";
    protected static final String CURATED_ACCOUNT_STATS_TABLE_NAME = "CURATED_ACCOUNT_STATS_TABLE_NAME";
    protected static final String FULL_REMATCH_PA = "FULL_REMATCH_PA";
    protected static final String REMATCHED_ACCOUNT_TABLE_NAME = "REMATCHED_ACCOUNT_TABLE_NAME";
    protected static final String ENRICHED_ACCOUNT_DIFF_TABLE_NAME = "ENRICHED_ACCOUNT_DIFF_TABLE_NAME";

    // store set of entities that are already published
    protected static final String PUBLISHED_ENTITIES = "PUBLISHED_ENTITIES";
    public static final String EXISTING_RECORDS = "EXISTING_RECORDS";
    public static final String UPDATED_RECORDS = "UPDATED_RECORDS";
    public static final String NEW_RECORDS = "NEW_RECORDS";
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

    protected static final String INPUT_SKIPPED_ATTRIBUTES_KEY = "INPUT_SKIPPED_ATTRIBUTES_KEY";

    public static final String PROCESS_ANALYTICS_DECISIONS_KEY = "PROCESS_ANALYTICS_DECISIONS_KEY";
    public static final String PROCESS_ANALYTICS_WARNING_KEY = "PROCESS_ANALYTICS_WARNING_KEY";
    public static final String CHOREOGRAPHER_CONTEXT_KEY = "CHOREOGRAPHER_CONTEXT_KEY";

    public static final String DATAQUOTA_LIMIT = "DATAQUOTA_LIMIT";
    public static final String ATTRIBUTE_QUOTA_LIMIT = "ATTRIBUTE_QUOTA_LIMIT";

    // tables to be carried over in restarted PA
    protected static final Set<String> TABLE_NAMES_FOR_PA_RETRY = Sets.newHashSet( //
            ENTITY_MATCH_ACCOUNT_TARGETTABLE, //
            ENTITY_MATCH_CONTACT_TARGETTABLE, //
            ENTITY_MATCH_CONTACT_ACCOUNT_TARGETTABLE, //
            ENTITY_MATCH_TXN_TARGETTABLE, //
            ENTITY_MATCH_TXN_ACCOUNT_TARGETTABLE, //
            ACCOUNT_DIFF_TABLE_NAME, //
            ACCOUNT_MASTER_TABLE_NAME, //
            FULL_ACCOUNT_TABLE_NAME, //
            ACCOUNT_EXPORT_TABLE_NAME, //
            ACCOUNT_FEATURE_TABLE_NAME, //
            ACCOUNT_PROFILE_TABLE_NAME, //
            ACCOUNT_SERVING_TABLE_NAME, //
            ACCOUNT_STATS_TABLE_NAME, //
            REMATCHED_ACCOUNT_TABLE_NAME, //
            ENRICHED_ACCOUNT_DIFF_TABLE_NAME, //
            CONTACT_SERVING_TABLE_NAME, //
            CONTACT_PROFILE_TABLE_NAME, //
            CONTACT_STATS_TABLE_NAME, //
            DAILY_TRXN_TABLE_NAME, //
            AGG_DAILY_TRXN_TABLE_NAME, //
            AGG_PERIOD_TRXN_TABLE_NAME, //
            PH_SERVING_TABLE_NAME, //
            PH_DEPIVOTED_TABLE_NAME, //
            PH_PROFILE_TABLE_NAME, //
            PH_STATS_TABLE_NAME, //
            CURATED_ACCOUNT_SERVING_TABLE_NAME, //
            CURATED_ACCOUNT_STATS_TABLE_NAME);
    protected static final Set<String> TABLE_NAME_LISTS_FOR_PA_RETRY = Collections.singleton(PERIOD_TRXN_TABLE_NAME);

    // extra context keys to be carried over in restarted PA, beyond table names
    // above
    protected static final Set<String> EXTRA_KEYS_FOR_PA_RETRY = Sets.newHashSet( //
            PA_TIMESTAMP, //
            ENTITY_MATCH_COMPLETED, //
            NEW_ENTITY_MATCH_ENVS, //
            FULL_REMATCH_PA, //
            NEW_RECORD_CUT_OFF_TIME);

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

    @Inject
    protected MetadataProxy metadataProxy;

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
        InternalResourceRestApiProxy proxy = getInternalResourceProxy();
        return proxy.findSourceFileByName(name, space.toString());
    }

    protected InternalResourceRestApiProxy getInternalResourceProxy() {
        return new InternalResourceRestApiProxy(getConfiguration().getInternalResourceHostPort());
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

        InternalResourceRestApiProxy proxy = getInternalResourceProxy();
        RetryTemplate template = RetryUtils.getExponentialBackoffRetryTemplate(3, 5000L, 2.0, null);
        template.execute(context -> {
            if (context.getRetryCount() >= 1) {
                log.warn("Last registering report for tenant {} failed. Retrying for {} times.", customerSpace,
                        context.getRetryCount());
            }
            proxy.registerReport(report, customerSpace.toString());
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

        InternalResourceRestApiProxy proxy = getInternalResourceProxy();
        return proxy.findReportByName(name, space.toString());
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
     *            list of context keys that contain a list of tables as
     *            serialized list of string
     * @return list of tables, null will be inserted if the corresponding table
     *         does not exist
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

    protected Table getTableSummaryFromKey(String customer, String tableNameCtxKey) {
        String tableName = getStringValueFromContext(tableNameCtxKey);
        return getTableSummary(customer, tableName);
    }

    private Table getTableSummary(String customer, String tableName) {
        if (StringUtils.isNotBlank(tableName)) {
            RetryTemplate retry = RetryUtils.getRetryTemplate(3);
            return retry.execute(ctx -> metadataProxy.getTableSummary(customer, tableName));
        }
        return null;
    }

    protected Set<String> getTableNamesForPaRetry() {
        return Stream.concat( //
                TABLE_NAMES_FOR_PA_RETRY.stream() //
                        .map(this::getStringValueFromContext), //
                TABLE_NAME_LISTS_FOR_PA_RETRY.stream() //
                        .flatMap(key -> {
                            List<String> tableNames = getListObjectFromContext(key, String.class);
                            if (CollectionUtils.isEmpty(tableNames)) {
                                return Stream.empty();
                            } else {
                                return tableNames.stream();
                            }
                        }) //
        ).filter(StringUtils::isNotBlank).collect(Collectors.toSet());
    }

    protected Set<String> getRenewableCtxKeys() {
        Set<String> renewableKeys = new HashSet<>(TABLE_NAMES_FOR_PA_RETRY);
        renewableKeys.addAll(TABLE_NAME_LISTS_FOR_PA_RETRY);
        renewableKeys.addAll(EXTRA_KEYS_FOR_PA_RETRY);
        return renewableKeys;
    }

}

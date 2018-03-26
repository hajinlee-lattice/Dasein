package com.latticeengines.workflow.exposed.build;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
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
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.util.WorkflowUtils;

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
    protected static final String SCORING_SCORE_FIELDS = "SCORING_SCORE_FIELDS";
    protected static final String SCORING_BUCKET_METADATA = "SCORING_BUCKET_METADATA";
    protected static final String SCORING_RESULT_TABLE_NAME = "SCORING_RESULT_TABLE_NAME";
    protected static final String SCORING_MODEL_ID = "SCORING_MODEL_ID";
    protected static final String SCORING_MODEL_TYPE = "SCORING_MODEL_TYPE";
    protected static final String SCORING_SOURCE_DIR = "SCORING_SOURCE_DIR";
    protected static final String SCORING_UNIQUEKEY_COLUMN = "SCORING_UNIQUEKEY_COLUMN";
    protected static final String ATTR_LEVEL_TYPE = "ATTR_LEVEL_TYPE";
    protected static final String IMPORT_DATA_APPLICATION_ID = "IMPORT_DATA_APPLICATION_ID";
    protected static final String ACTIVATE_MODEL_IDS = "ACTIVATE_MODEL_IDS";
    protected static final String EXPORT_DATA_APPLICATION_ID = "EXPORT_DATA_APPLICATION_ID";
    protected static final String EXPORT_TABLE_NAME = "EXPORT_TABLE_NAME";
    protected static final String EXPORT_INPUT_PATH = "EXPORT_INPUT_PATH";
    protected static final String EXPORT_OUTPUT_PATH = "EXPORT_OUTPUT_PATH";
    protected static final String TRANSFORMATION_GROUP_NAME = "TRANSFORMATION_GROUP_NAME";
    protected static final String COLUMN_RULE_RESULTS = "COLUMN_RULE_RESULTS";
    protected static final String ROW_RULE_RESULTS = "ROW_RULE_RESULTS";
    protected static final String EVENT_TO_MODELID = "EVENT_TO_MODELID";
    protected static final String DATA_RULES = "DATA_RULES";
    protected static final String SOURCE_IMPORT_TABLE = "SOURCE_IMPORT_TABLE_NAME";
    protected static final String TRANSFORM_PIPELINE_VERSION = "TRANSFORM_PIPELINE_VERSION";

    // CDL
    public static final String CONSOLIDATE_INPUT_IMPORTS = "CONSOLIDATE_INPUT_IMPORTS";
    public static final String CDL_ACTIVE_VERSION = "CDL_ACTIVE_VERSION";
    public static final String CUSTOMER_SPACE = "CUSTOMER_SPACE";
    public static final String TABLE_GOING_TO_REDSHIFT = "TABLE_GOING_TO_REDSHIFT";
    public static final String ENTITIES_WITH_SCHEMA_CHANGE = "ENTITIES_WITH_SCHEMA_CHANGE";
    public static final String RATING_MODELS = "RATING_MODELS";
    protected static final String RULE_RAW_RATING_TABLE_NAME = "RULE_RAW_RATING_TABLE_NAME";
    protected static final String AI_RAW_RATING_TABLE_NAME = "AI_RAW_RATING_TABLE_NAME";
    protected static final String APPEND_TO_REDSHIFT_TABLE = "APPEND_TO_REDSHIFT_TABLE";
    protected static final String REDSHIFT_EXPORT_REPORT = "REDSHIFT_EXPORT_REPORT";
    protected static final String CDL_INACTIVE_VERSION = "CDL_INACTIVE_VERSION";
    protected static final String CDL_EVALUATION_DATE = "CDL_EVALUATION_DATE";
    protected static final String STATS_TABLE_NAMES = "STATS_TABLE_NAMES";
    protected static final String ENTITY_DIFF_TABLES = "ENTITY_DIFF_TABLES";
    protected static final String RESET_ENTITIES = "RESET_ENTITIES";
    protected static final String EVALUATION_PERIOD = "EVALUATION_PERIOD";
    protected static final String RATING_LIFTS = "RATING_LIFTS";
    protected static final String PREDICTION_TYPES = "PREDICTION_TYPES";

    protected static final String CUSTOM_EVENT_IMPORT = "CUSTOM_EVENT_IMPORT";
    protected static final String CUSTOM_EVENT_MATCH_ACCOUNT = "CUSTOM_EVENT_MATCH_ACCOUNT";
    protected static final String CUSTOM_EVENT_MATCH_ACCOUNT_ID = "CUSTOM_EVENT_MATCH_ACCOUNT_ID";
    protected static final String CUSTOM_EVENT_MATCH_WITHOUT_ACCOUNT_ID = "CUSTOM_EVENT_MATCH_WITHOUT_ACCOUNT_ID";

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
            }
            i++;

            if (i == maxTries) {
                break;
            }
        } while (!YarnUtils.TERMINAL_STATUS.contains(status.getStatus()));

        if (status.getStatus() != FinalApplicationStatus.SUCCEEDED) {
            throw new LedpException(LedpCode.LEDP_28015, new String[] { appId, status.getStatus().toString() });
        }

    }

    protected String getHdfsDir(String path) {
        String[] tokens = StringUtils.split(path, "/");
        String[] newTokens = null;
        ;
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
        SourceFile sourceFile = proxy.findSourceFileByName(name, space.toString());
        return sourceFile;
    }

    protected InternalResourceRestApiProxy getInternalResourceProxy() {
        return new InternalResourceRestApiProxy(getConfiguration().getInternalResourceHostPort());
    }

    protected void registerReport(CustomerSpace customerSpace, Report report) {
        @SuppressWarnings("unchecked")
        Map<String, String> map = getObjectFromContext(WorkflowContextConstants.REPORTS, Map.class);

        if (map == null) {
            map = new HashMap<String, String>();
        }

        map.put(report.getPurpose().getKey(), report.getName());

        saveReport(map);
        putObjectInContext(WorkflowContextConstants.REPORTS, map);

        InternalResourceRestApiProxy proxy = getInternalResourceProxy();
        proxy.registerReport(report, customerSpace.toString());
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
        String ns = StringUtils.isEmpty(parentNamespace) ? workflowConfigClass.getSimpleName()
                : parentNamespace + "." + workflowConfigClass.getSimpleName();
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
        Map<String, BaseStepConfiguration> result = registry.entrySet().stream()
                .collect(Collectors.toMap(e -> parentNamespace + "." + e.getKey(), e -> {
                    String namespace = e.getKey();
                    if (StringUtils.isNotEmpty(parentNamespace)) {
                        namespace = parentNamespace + "." + namespace;
                    }
                    BaseStepConfiguration step = getObjectFromContext(namespace, BaseStepConfiguration.class);
                    if (step == null) {
                        step = getConfigurationFromJobParameters(namespace);
                        if (step == null) {
                            step = JsonUtils.deserialize(e.getValue(), BaseStepConfiguration.class);
                        }
                    }
                    return step;
                }));
        return result;
    }
}

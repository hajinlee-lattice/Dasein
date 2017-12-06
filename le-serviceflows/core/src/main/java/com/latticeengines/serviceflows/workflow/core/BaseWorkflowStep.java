package com.latticeengines.serviceflows.workflow.core;

import java.lang.reflect.Method;
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
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.PivotValuesLookup;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ModelStepConfiguration;
import com.latticeengines.domain.exposed.util.ModelingUtils;
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
import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;

public abstract class BaseWorkflowStep<T extends BaseStepConfiguration> extends AbstractStep<T> {

    protected static final Logger log = LoggerFactory.getLogger(BaseWorkflowStep.class);

    protected static final String PREMATCH_EVENT_TABLE = "PREMATCH_EVENT_TABLE";
    protected static final String FILTER_EVENT_TABLE = "FILTER_EVENT_TABLE";
    protected static final String MATCH_FETCH_ONLY = "MATCH_FETCH_ONLY";
    protected static final String EVENT_TABLE = "EVENT_TABLE";
    protected static final String EVENT_COLUMN = "EVENT_COLUMN";
    protected static final String DB_CREDS = "DB_CREDS";
    protected static final String MATCH_COMMAND_ID = "MATCH_COMMAND_ID";
    protected static final String MATCH_COMMAND = "MATCH_COMMAND";
    protected static final String MATCH_TABLE = "MATCH_TABLE";
    protected static final String MATCH_RESULT_TABLE = "MATCH_RESULT_TABLE";
    protected static final String MATCH_PREDEFINED_SELECTION = "MATCH_PREDEFINED_SELECTION";
    protected static final String MATCH_PREDEFINED_SELECTION_VERSION = "MATCH_PREDEFINED_SELECTION_VERSION";
    protected static final String MATCH_CUSTOMIZED_SELECTION = "MATCH_CUSTOMIZED_SELECTION";
    protected static final String MODELING_SERVICE_EXECUTOR_BUILDER = "MODELING_SERVICE_EXECUTOR_BUILDER";
    protected static final String MODEL_APP_IDS = "MODEL_APP_IDS";
    protected static final String MODEL_AVG_PROBABILITY = "MODEL_AVG_PROBABILITY";
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
    public static final String SERVING_STORE_IN_STATS = "SERVING_STORE_IN_STATS";
    public static final String CONSOLIDATE_INPUT_IMPORTS = "CONSOLIDATE_INPUT_IMPORTS";
    public static final String CDL_ACTIVE_VERSION = "CDL_ACTIVE_VERSION";
    public static final String CUSTOMER_SPACE = "CUSTOMER_SPACE";
    public static final String TABLE_GOING_TO_REDSHIFT = "TABLE_GOING_TO_REDSHIFT";
    protected static final String APPEND_TO_REDSHIFT_TABLE = "APPEND_TO_REDSHIFT_TABLE";
    protected static final String REDSHIFT_EXPORT_REPORT = "REDSHIFT_EXPORT_REPORT";
    protected static final String CDL_INACTIVE_VERSION = "CDL_INACTIVE_VERSION";
    protected static final String STATS_TABLE_NAMES = "STATS_TABLE_NAMES";
    protected static final String ENTITY_DIFF_TABLES = "ENTITY_DIFF_TABLES";

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

    private String getDataCompositionContents(Table eventTable) {
        DataComposition dataComposition = new DataComposition();
        Map.Entry<Map<String, FieldSchema>, List<TransformDefinition>> transforms = eventTable
                .getRealTimeTransformationMetadata();
        dataComposition.fields = transforms.getKey();
        dataComposition.transforms = transforms.getValue();
        return JsonUtils.serialize(dataComposition);
    }

    protected ModelingServiceExecutor.Builder createModelingServiceExecutorBuilder(
            ModelStepConfiguration modelStepConfiguration, Table eventTable) {
        String metadataContents = JsonUtils.serialize(eventTable.getModelingMetadata());
        try {
            PivotValuesLookup pivotValues = ModelingUtils.getPivotValues(yarnConfiguration,
                    modelStepConfiguration.getPivotArtifactPath());
            metadataContents = ModelingUtils.addPivotValuesToMetadataContent(eventTable.getModelingMetadata(),
                    pivotValues);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }

        String dataCompositionContents = getDataCompositionContents(eventTable);

        List<DataRule> dataRules = null;
        if (executionContext.containsKey(DATA_RULES)) {
            dataRules = getListObjectFromContext(DATA_RULES, DataRule.class);
        } else {
            dataRules = modelStepConfiguration.getDataRules();
        }

        ModelingServiceExecutor.Builder bldr = new ModelingServiceExecutor.Builder();
        bldr.sampleSubmissionUrl("/modeling/samples") //
                .profileSubmissionUrl("/modeling/profiles") //
                .modelSubmissionUrl("/modeling/models") //
                .retrieveFeaturesUrl("/modeling/features") //
                .retrieveJobStatusUrl("/modeling/jobs/%s") //
                .retrieveModelingJobStatusUrl("/modeling/modelingjobs/%s") //
                .modelingServiceHostPort(modelStepConfiguration.getMicroServiceHostPort()) //
                .modelingServiceHdfsBaseDir(modelStepConfiguration.getModelingServiceHdfsBaseDir()) //
                .customer(modelStepConfiguration.getCustomerSpace().toString()) //
                .metadataContents(metadataContents) //
                .dataCompositionContents(dataCompositionContents) //
                .yarnConfiguration(yarnConfiguration) //
                .hdfsDirToSample(getHdfsDir(eventTable.getExtracts().get(0).getPath())) //
                .table(eventTable.getName()) //
                .modelProxy(modelProxy) //
                .jobProxy(jobProxy) //
                .dataRules(dataRules) //
                .runTimeParams(modelStepConfiguration.getRunTimeParams()) //
                .setModelSummaryProvenance(modelStepConfiguration.getModelSummaryProvenance()) //
                .productType(modelStepConfiguration.getProductType());

        return bldr;
    }

    protected void saveOutputValue(String key, String val) {
        putOutputValue(key, val);
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(jobId);
        workflowJob.setOutputContextValue(key, val);
        workflowJobEntityMgr.updateWorkflowJob(workflowJob);
    }

    protected void saveReport(Map<String, String> map) {
        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(jobId);
        for (Map.Entry<String, String> entry : map.entrySet()) {
            workflowJob.setReportName(entry.getKey(), entry.getValue());
        }
        workflowJobEntityMgr.updateWorkflowJob(workflowJob);
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

    protected void skipEmbeddedWorkflow(Class<? extends WorkflowConfiguration> workflowClass) {
        Map<String, BaseStepConfiguration> stepConfigMap = getStepConfigMapInWorkflow(workflowClass);
        stepConfigMap.forEach((name, step) -> {
            step.setSkipStep(true);
            putObjectInContext(name, step);
            log.info("Set step " + name + " to be skipped.");
        });
    }

    protected Map<String, BaseStepConfiguration> getStepConfigMapInWorkflow(
            Class<? extends WorkflowConfiguration> workflowClass) {
        WorkflowConfiguration workflow = getObjectFromContext(workflowClass.getName(), workflowClass);
        if (workflow == null) {
            log.warn("There is no workflow configuration of class " + workflowClass.getSimpleName() + " in context.");
            try {
                Class<?> builderClass = Arrays.stream(workflowClass.getDeclaredClasses())
                        .filter(c -> c.getSimpleName().equals("Builder")).distinct().findFirst().orElse(null);
                Object builder = builderClass.newInstance();
                Method build = builderClass.getMethod("build", new Class<?>[] {});
                workflow = (WorkflowConfiguration) build.invoke(builder);
            } catch (Exception e) {
                throw new RuntimeException(
                        String.format("Can't instantiate workflow configuration %s", workflowClass.getSimpleName()), e);
            }
        }
        Map<String, String> registry = workflow.getConfigRegistry();
        return registry.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> {
            Class<?> configClass = null;
            try {
                configClass = Class.forName(e.getKey());
            } catch (ClassNotFoundException e1) {
                throw new RuntimeException(String.format("unable to find class %s", e.getKey()), e1);
            }
            BaseStepConfiguration step = (BaseStepConfiguration) getObjectFromContext(e.getKey(), configClass);
            if (step == null) {
                step = (BaseStepConfiguration) getConfigurationFromJobParameters(configClass);
                if (step == null) {
                    step = (BaseStepConfiguration) JsonUtils.deserialize(e.getValue(), configClass);
                }
            }
            return step;
        }));

    }
}

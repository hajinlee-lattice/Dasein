package com.latticeengines.serviceflows.workflow.core;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.PivotValuesLookup;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.scoringapi.DataComposition;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.util.ModelingUtils;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.dataplatform.JobProxy;
import com.latticeengines.proxy.exposed.dataplatform.ModelProxy;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.serviceflows.workflow.modeling.ModelStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

public abstract class BaseWorkflowStep<T extends BaseStepConfiguration> extends AbstractStep<T> {

    protected static final Log log = LogFactory.getLog(BaseWorkflowStep.class);

    protected static final String PREMATCH_EVENT_TABLE = "PREMATCH_EVENT_TABLE";
    protected static final String EVENT_TABLE = "EVENT_TABLE";
    protected static final String EVENT_COLUMN = "EVENT_COLUMN";
    protected static final String DB_CREDS = "DB_CREDS";
    protected static final String MATCH_COMMAND_ID = "MATCH_COMMAND_ID";
    protected static final String MATCH_TABLE = "MATCH_TABLE";
    protected static final String MATCH_ROOT_UID = "MATCH_ROOT_UID";
    protected static final String MATCH_RESULT_TABLE = "MATCH_RESULT_TABLE";
    protected static final String MATCH_PREDEFINED_SELECTION = "MATCH_PREDEFINED_SELECTION";
    protected static final String MATCH_PREDEFINED_SELECTION_VERSION = "MATCH_PREDEFINED_SELECTION_VERSION";
    protected static final String MATCH_CUSTOMIZED_SELECTION = "MATCH_CUSTOMIZED_SELECTION";
    protected static final String MODELING_SERVICE_EXECUTOR_BUILDER = "MODELING_SERVICE_EXECUTOR_BUILDER";
    protected static final String MODEL_APP_IDS = "MODEL_APP_IDS";
    protected static final String MODEL_AVG_PROBABILITY = "MODEL_AVG_PROBABILITY";
    protected static final String SCORING_RESULT_TABLE_NAME = "SCORING_RESULT_TABLE_NAME";
    protected static final String SCORING_MODEL_ID = "SCORING_MODEL_ID";
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

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected ModelProxy modelProxy;

    @Autowired
    protected JobProxy jobProxy;

    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor();
    protected List<ClientHttpRequestInterceptor> addMagicAuthHeaders = Arrays
            .asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader });
    protected RestTemplate restTemplate = new RestTemplate();

    @Override
    public boolean setup() {
        boolean result = super.setup();
        SSLUtils.turnOffSslChecking();
        restTemplate.setInterceptors(addMagicAuthHeaders);
        return result;
    }

    protected void waitForAppId(String appId, String microServiceHostPort) {
        log.info(String.format("Waiting for appId: %s", appId));

        JobStatus status;
        int maxTries = 10000;
        int i = 0;
        do {
            String url = String.format(microServiceHostPort + "/modeling/jobs/%s", appId);
            status = restTemplate.getForObject(url, JobStatus.class);
            try {
                Thread.sleep(1000L);
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

    @SuppressWarnings("unchecked")
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
            dataRules = (List<DataRule>) executionContext.get(DATA_RULES);
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
                .runTimeParams(modelStepConfiguration.runTimeParams()) //
                .productType(modelStepConfiguration.getProductType());

        return bldr;
    }

    @SuppressWarnings("unchecked")
    protected void putOutputValue(String key, String val) {
        Map<String, String> map = getObjectFromContext(WorkflowContextConstants.OUTPUTS, Map.class);
        if (map == null) {
            map = new HashMap<>();
        }
        map.put(key, val);
        putObjectInContext(WorkflowContextConstants.OUTPUTS, map);
    }

    @SuppressWarnings("unchecked")
    protected <V> V getObjectFromContext(String key, Class<V> clazz) {
        return (V) executionContext.get(key);
    }

    protected <V> void putObjectInContext(String key, V val) {
        executionContext.put(key, val);
    }

    protected String getStringValueFromContext(String key) {
        try {
            return executionContext.getString(key);
        } catch (ClassCastException e) {
            return null;
        }
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
        Map<ReportPurpose, String> map = getObjectFromContext(WorkflowContextConstants.REPORTS, Map.class);

        if (map == null) {
            map = new HashMap<ReportPurpose, String>();
        }

        map.put(report.getPurpose(), report.getName());
        putObjectInContext(WorkflowContextConstants.REPORTS, map);

        InternalResourceRestApiProxy proxy = new InternalResourceRestApiProxy(getConfiguration()
                .getInternalResourceHostPort());
        proxy.registerReport(report, customerSpace.toString());
    }

    protected Report retrieveReport(CustomerSpace space, ReportPurpose purpose) {
        @SuppressWarnings("unchecked")
        Map<ReportPurpose, String> map = getObjectFromContext(WorkflowContextConstants.REPORTS, Map.class);

        if (map == null) {
            return null;
        }

        String name = map.get(purpose);
        if (name == null) {
            return null;
        }

        InternalResourceRestApiProxy proxy = getInternalResourceProxy();
        return proxy.findReportByName(name, space.toString());
    }

    protected Double getDoubleValueFromContext(String key) {
        try {
            return executionContext.getDouble(key);
        } catch (ClassCastException e) {
            return null;
        }
    }

}

package com.latticeengines.serviceflows.workflow.core;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.serviceflows.workflow.modeling.ModelStepConfiguration;
import com.latticeengines.workflow.exposed.WorkflowContextConstants;
import com.latticeengines.workflow.exposed.build.AbstractStep;

public abstract class BaseWorkflowStep<T extends BaseStepConfiguration> extends AbstractStep<T> {

    private static final Log log = LogFactory.getLog(BaseWorkflowStep.class);

    protected static final String PREMATCH_EVENT_TABLE = "PREMATCH_EVENT_TABLE";
    protected static final String EVENT_TABLE = "EVENT_TABLE";
    protected static final String EVENT_COLUMN = "EVENT_COLUMN";
    protected static final String DB_CREDS = "DB_CREDS";
    protected static final String MATCH_COMMAND_ID = "MATCH_COMMAND_ID";
    protected static final String MATCH_TABLE = "MATCH_TABLE";
    protected static final String MODELING_SERVICE_EXECUTOR_BUILDER = "MODELING_SERVICE_EXECUTOR_BUILDER";
    protected static final String MODEL_APP_IDS = "MODEL_APP_IDS";
    protected static final String MODEL_AVG_PROBABILITY = "MODEL_AVG_PROBABILITY";
    protected static final String SCORING_RESULT_TABLE = "SCORING_RESULT_TABLE";
    protected static final String SCORING_MODEL_ID = "SCORING_MODEL_ID";
    protected static final String SCORING_SOURCE_DIR = "SCORING_SOURCE_DIR";
    protected static final String SCORING_UNIQUEKEY_COLUMN = "SCORING_UNIQUEKEY_COLUMN";
    protected static final String ATTR_LEVEL_TYPE = "ATTR_LEVEL_TYPE";

    @Autowired
    protected Configuration yarnConfiguration;

    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor();
    protected List<ClientHttpRequestInterceptor> addMagicAuthHeaders = Arrays
            .asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader });
    protected RestTemplate restTemplate = new RestTemplate();

    @Override
    public boolean setup() {
        boolean result = super.setup();
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

    protected ModelingServiceExecutor.Builder createModelingServiceExecutorBuilder(
            ModelStepConfiguration modelStepConfiguration, Table eventTable) {
        String metadataContents = JsonUtils.serialize(eventTable.getModelingMetadata());

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
                .yarnConfiguration(yarnConfiguration) //
                .hdfsDirToSample(getHdfsDir(eventTable.getExtracts().get(0).getPath())) //
                .table(eventTable.getName());

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

    @SuppressWarnings("unchecked")
    protected void registerSourceFileInContext(SourceFile sourceFile) {
        @SuppressWarnings("unchecked")
        Set<String> sourceFiles = getObjectFromContext(WorkflowContextConstants.SOURCE_FILES, Set.class);

        if (sourceFiles == null) {
            sourceFiles = new HashSet<String>();
        }

        sourceFiles.add(sourceFile.getName());
        putObjectInContext(WorkflowContextConstants.SOURCE_FILES, sourceFiles);
    }

    protected SourceFile retrieveSourceFile(CustomerSpace space, String name) {
        if (name == null) {
            return null;
        }
        InternalResourceRestApiProxy proxy = getInternalResourceProxy();
        SourceFile sourceFile = proxy.findSourceFileByName(name, space.toString());
        registerSourceFileInContext(sourceFile);
        return sourceFile;
    }

    protected InternalResourceRestApiProxy getInternalResourceProxy() {
        return new InternalResourceRestApiProxy(getConfiguration().getInternalResourceHostPort());
    }

    protected void registerReportInContext(Report report) {
        @SuppressWarnings("unchecked")
        Set<String> reports = getObjectFromContext(WorkflowContextConstants.REPORTS, Set.class);

        if (reports == null) {
            reports = new HashSet<String>();
        }

        reports.add(report.getName());
        putObjectInContext(WorkflowContextConstants.REPORTS, reports);
    }

    protected Double getDoubleValueFromContext(String key) {
        try {
            return executionContext.getDouble(key);
        } catch (ClassCastException e) {
            return null;
        }

    }

}

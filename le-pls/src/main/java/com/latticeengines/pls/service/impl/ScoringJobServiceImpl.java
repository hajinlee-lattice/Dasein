package com.latticeengines.pls.service.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.ScoringJobService;
import com.latticeengines.pls.workflow.ImportAndRTSBulkScoreWorkflowSubmitter;
import com.latticeengines.pls.workflow.RTSBulkScoreWorkflowSubmitter;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("scoringJobService")
public class ScoringJobServiceImpl implements ScoringJobService {
    private static final Logger log = LoggerFactory.getLogger(ScoringJobServiceImpl.class);

    @Value("${pls.scoring.use.rtsapi}")
    private boolean useRtsApiDefaultValue;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private RTSBulkScoreWorkflowSubmitter rtsBulkScoreWorkflowSubmitter;

    @Autowired
    private ImportAndRTSBulkScoreWorkflowSubmitter importAndRTSBulkScoreWorkflowSubmitter;

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    public List<Job> getJobs(String modelId) {
        Tenant tenantWithPid = getTenant();
        log.debug("Finding jobs for " + tenantWithPid.toString() + " with pid " + tenantWithPid.getPid() + " and model "
                + modelId);
        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(tenantWithPid.getPid());
        List<Job> ret = new ArrayList<>();
        for (Job job : jobs) {
            if (job.getJobType().equals("scoreWorkflow") || job.getJobType().equals("importMatchAndScoreWorkflow")
                    || job.getJobType().equals("rtsBulkScoreWorkflow")
                    || job.getJobType().equals("importAndRTSBulkScoreWorkflow")) {
                String jobModelId = job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_ID);
                ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
                String jobModelName = modelSummary != null ? modelSummary.getDisplayName() : null;
                if (jobModelId != null && jobModelId.equals(modelId)) {
                    job.getInputs().put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, jobModelName);
                    ret.add(job);
                }
            }
        }
        return ret;

    }

    @Override
    public InputStream getScoreResults(String workflowJobId) {
        return getResultFile(workflowJobId, WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH);
    }

    @Override
    public InputStream getPivotScoringFile(String workflowJobId) {
        return getResultFile(workflowJobId, WorkflowContextConstants.Outputs.PIVOT_SCORE_EVENT_EXPORT_PATH);
    }

    private InputStream getResultFile(String workflowJobId, String resultFileType) {
        Job job = workflowProxy.getWorkflowExecution(workflowJobId);
        if (job == null) {
            throw new LedpException(LedpCode.LEDP_18104, new String[] { workflowJobId });
        }

        String path = job.getOutputs().get(resultFileType);

        if (path == null) {
            throw new LedpException(LedpCode.LEDP_18103, new String[] { workflowJobId });
        }

        try {
            String hdfsDir = StringUtils.substringBeforeLast(path, "/");
            String filePrefix = StringUtils.substringAfterLast(path, "/");
            List<String> paths = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsDir, filePrefix + ".*");
            if (CollectionUtils.isEmpty(paths)) {
                throw new LedpException(LedpCode.LEDP_18103, new String[] { workflowJobId });
            }
            return HdfsUtils.getInputStream(yarnConfiguration, paths.get(0));

        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18102, e, new String[] { workflowJobId });
        }
    }

    @Override
    public String getResultScoreFileName(String workflowJobId) {
        Job job = workflowProxy.getWorkflowExecution(workflowJobId);
        if (job == null) {
            throw new LedpException(LedpCode.LEDP_18104, new String[] { workflowJobId });
        }

        String path = job.getOutputs().get(WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH);

        if (path == null) {
            throw new LedpException(LedpCode.LEDP_18103, new String[] { workflowJobId });
        }
        String fileName = StringUtils.substringAfterLast(path, "/");
        if (!fileName.contains("_scored")) {
            return "scored.csv";
        }
        return StringUtils.substringBeforeLast(fileName, "_scored") + "_scored.csv";
    }

    @Override
    public String getResultPivotScoreFileName(String workflowJobId) {
        Job job = workflowProxy.getWorkflowExecution(workflowJobId);
        if (job == null) {
            throw new LedpException(LedpCode.LEDP_18104, new String[] { workflowJobId });
        }

        String path = job.getOutputs().get(WorkflowContextConstants.Outputs.PIVOT_SCORE_EVENT_EXPORT_PATH);

        if (path == null) {
            throw new LedpException(LedpCode.LEDP_18103, new String[] { workflowJobId });
        }
        String fileName = StringUtils.substringAfterLast(path, "/");
        return StringUtils.substringBeforeLast(fileName, "_pivoted") + "_pivoted.csv";
    }

    @Override
    public String scoreTestingData(String modelId, String fileName, Boolean performEnrichment, Boolean debug) {
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        boolean enableLeadEnrichment = performEnrichment == null ? false : performEnrichment.booleanValue();
        boolean enableDebug = debug == null ? false : debug.booleanValue();
        return scoreTestingDataUsingRtsApi(modelSummary, fileName, enableLeadEnrichment, enableDebug);
    }

    @Override
    public String scoreTrainingData(String modelId, Boolean performEnrichment, Boolean debug) {
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        boolean enableLeadEnrichment = performEnrichment == null ? false : performEnrichment.booleanValue();
        boolean enableDebug = debug == null ? false : debug.booleanValue();
        return scoreTrainingDataUsingRtsApi(modelSummary, enableLeadEnrichment, enableDebug);
    }

    private Tenant getTenant() {
        Tenant tenant = MultiTenantContext.getTenant();
        return tenantEntityMgr.findByTenantId(tenant.getId());
    }

    private String scoreTrainingDataUsingRtsApi(ModelSummary modelSummary, boolean enableLeadEnrichment,
            boolean debug) {
        if (modelSummary.getTrainingTableName() == null) {
            throw new LedpException(LedpCode.LEDP_18100, new String[] { modelSummary.getId() });
        }

        return rtsBulkScoreWorkflowSubmitter.submit(modelSummary.getId(), modelSummary.getTrainingTableName(),
                enableLeadEnrichment, "Training Data", debug).toString();
    }

    private String scoreTestingDataUsingRtsApi(ModelSummary modelSummary, String fileName, boolean enableLeadEnrichment,
            boolean debug) {
        return importAndRTSBulkScoreWorkflowSubmitter
                .submit(modelSummary.getId(), fileName, enableLeadEnrichment, debug).toString();
    }

    @Override
    public InputStream getScoringErrorStream(String workflowJobId) {
        Job job = workflowProxy.getWorkflowExecution(workflowJobId);
        if (job == null) {
            throw new LedpException(LedpCode.LEDP_18104, new String[] { workflowJobId });
        }

        String path = job.getOutputs().get(WorkflowContextConstants.Outputs.ERROR_OUTPUT_PATH);

        try {
            if (path == null) {
                return new ByteArrayInputStream(new byte[0]);
            }
            return HdfsUtils.getInputStream(yarnConfiguration, path);

        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18102, e, new String[] { workflowJobId });
        }
    }
}

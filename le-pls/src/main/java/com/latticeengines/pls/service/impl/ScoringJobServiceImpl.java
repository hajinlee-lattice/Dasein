package com.latticeengines.pls.service.impl;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.ScoringJobService;
import com.latticeengines.pls.workflow.ImportAndRTSBulkScoreWorkflowSubmitter;
import com.latticeengines.pls.workflow.ImportMatchAndScoreWorkflowSubmitter;
import com.latticeengines.pls.workflow.RTSBulkScoreWorkflowSubmitter;
import com.latticeengines.pls.workflow.ScoreWorkflowSubmitter;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("scoringJobService")
public class ScoringJobServiceImpl implements ScoringJobService {
    private static final Log log = LogFactory.getLog(ScoringJobServiceImpl.class);

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
    private ScoreWorkflowSubmitter scoreWorkflowSubmitter;

    @Autowired
    private ImportMatchAndScoreWorkflowSubmitter importMatchAndScoreWorkflowSubmitter;

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    public List<Job> getJobs(String modelId) {
        Tenant tenantWithPid = getTenant();
        log.info("Finding jobs for " + tenantWithPid.toString() + " with pid " + tenantWithPid.getPid() + " and model "
                + modelId);
        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(tenantWithPid.getPid());
        List<Job> ret = new ArrayList<>();
        for (Job job : jobs) {
            if (job.getJobType().equals("scoreWorkflow") || job.getJobType().equals("importMatchAndScoreWorkflow")
                    || job.getJobType().equals("rtsBulkScoreWorkflow")
                    || job.getJobType().equals("importAndRTSBulkScoreWorkflow")) {
                String jobModelId = job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_ID);
                if (jobModelId != null && jobModelId.equals(modelId)) {
                    ret.add(job);
                }
            }
        }
        return ret;

    }

    @Override
    public InputStream getResults(String workflowJobId) {
        Job job = workflowProxy.getWorkflowExecution(workflowJobId);
        if (job == null) {
            throw new LedpException(LedpCode.LEDP_18104, new String[] { workflowJobId });
        }

        String path = job.getOutputs().get(WorkflowContextConstants.Outputs.EXPORT_OUTPUT_PATH);

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
    public String scoreTestingData(String modelId, String fileName, Boolean useRts, Boolean performEnrichment) {
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        boolean useRtsApi = false;
        boolean enableLeadEnrichment = performEnrichment == null ? false : performEnrichment.booleanValue();

        if (enableLeadEnrichment || modelSummary.getModelType().equals(ModelType.PMML.getModelType())) {
            useRtsApi = true;
        }

        if (useRtsApi) {
            return scoreTestingDataUsingRtsApi(modelSummary, fileName, enableLeadEnrichment);
        }
        return scoreTestingData(modelId, fileName);
    }

    @Override
    public String scoreTrainingData(String modelId, Boolean useRts, Boolean performEnrichment) {
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        boolean useRtsApi = false;
        boolean enableLeadEnrichment = performEnrichment == null ? false : performEnrichment.booleanValue();

        if (enableLeadEnrichment || modelSummary.getModelType().equals(ModelType.PMML.getModelType())) {
            useRtsApi = true;
        }

        if (useRtsApi) {
            return scoreTrainingDataUsingRtsApi(modelSummary, enableLeadEnrichment);

        }
        return scoreTrainingData(modelSummary);
    }

    private Tenant getTenant() {
        Tenant tenant = MultiTenantContext.getTenant();
        return tenantEntityMgr.findByTenantId(tenant.getId());
    }

    private String scoreTrainingData(ModelSummary modelSummary) {
        if (modelSummary.getTrainingTableName() == null) {
            throw new LedpException(LedpCode.LEDP_18100, new String[] { modelSummary.getId() });
        }

        String transformationGroupName = modelSummary.getTransformationGroupName();
        if (transformationGroupName == null) {
            throw new LedpException(LedpCode.LEDP_18108, new String[] { modelSummary.getId() });
        }

        return scoreWorkflowSubmitter.submit(modelSummary.getId(), modelSummary.getTrainingTableName(), "Training Data",
                TransformationGroup.fromName(transformationGroupName)).toString();
    }

    private String scoreTestingData(String modelId, String fileName) {
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        String transformationGroupName = modelSummary.getTransformationGroupName();
        if (transformationGroupName == null) {
            throw new LedpException(LedpCode.LEDP_18108, new String[] { modelId });
        }
        return importMatchAndScoreWorkflowSubmitter.submit(modelId, fileName,
                TransformationGroup.fromName(transformationGroupName)).toString();
    }

    private String scoreTrainingDataUsingRtsApi(ModelSummary modelSummary, boolean enableLeadEnrichment) {
        if (modelSummary.getTrainingTableName() == null) {
            throw new LedpException(LedpCode.LEDP_18100, new String[] { modelSummary.getId() });
        }

        return rtsBulkScoreWorkflowSubmitter.submit(modelSummary.getId(), modelSummary.getTrainingTableName(), enableLeadEnrichment,
                "Training Data").toString();
    }

    private String scoreTestingDataUsingRtsApi(ModelSummary modelSummary, String fileName, boolean enableLeadEnrichment) {
        return importAndRTSBulkScoreWorkflowSubmitter.submit(modelSummary.getId(), fileName, enableLeadEnrichment).toString();
    }
}

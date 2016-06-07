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
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.ScoringJobService;
import com.latticeengines.pls.workflow.ImportMatchAndScoreWorkflowSubmitter;
import com.latticeengines.pls.workflow.RTSBulkScoreWorkflowSubmitter;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("scoringJobService")
public class ScoringJobServiceImpl implements ScoringJobService {
    private static final Log log = LogFactory.getLog(ScoringJobServiceImpl.class);

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private RTSBulkScoreWorkflowSubmitter rtsBulkScoreWorkflowSubmitter;

    @Autowired
    private ImportMatchAndScoreWorkflowSubmitter importMatchAndScoreWorkflowSubmitter;

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    public String scoreTrainingData(String modelId) {
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        if (modelSummary.getTrainingTableName() == null) {
            throw new LedpException(LedpCode.LEDP_18100, new String[] { modelId });
        }

        String transformationGroupName = modelSummary.getTransformationGroupName();
        if (transformationGroupName == null) {
            throw new LedpException(LedpCode.LEDP_18108, new String[] { modelId });
        }

        return rtsBulkScoreWorkflowSubmitter.submit(modelId, modelSummary.getTrainingTableName(), "Training Data")
                .toString();
    }

    @Override
    public String scoreTestingData(String modelId, String fileName) {
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        String transformationGroupName = modelSummary.getTransformationGroupName();
        if (transformationGroupName == null) {
            throw new LedpException(LedpCode.LEDP_18108, new String[] { modelId });
        }
        return importMatchAndScoreWorkflowSubmitter.submit(modelId, fileName,
                TransformationGroup.fromName(transformationGroupName)).toString();
    }

    @Override
    public List<Job> getJobs(String modelId) {
        Tenant tenantWithPid = getTenant();
        log.info("Finding jobs for " + tenantWithPid.toString() + " with pid " + tenantWithPid.getPid() + " and model "
                + modelId);
        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(tenantWithPid.getPid());
        List<Job> ret = new ArrayList<>();
        for (Job job : jobs) {
            if (job.getJobType().equals("scoreWorkflow") || job.getJobType().equals("importMatchAndScoreWorkflow")) {
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

    private Tenant getTenant() {
        Tenant tenant = MultiTenantContext.getTenant();
        return tenantEntityMgr.findByTenantId(tenant.getId());
    }

}

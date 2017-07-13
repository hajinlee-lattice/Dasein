package com.latticeengines.datacloudapi.engine.orchestration.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.service.PropDataTenantService;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.etl.orchestration.entitymgr.OrchestrationEntityMgr;
import com.latticeengines.datacloud.etl.orchestration.entitymgr.OrchestrationProgressEntityMgr;
import com.latticeengines.datacloud.etl.orchestration.service.OrchestrationProgressService;
import com.latticeengines.datacloud.etl.orchestration.service.OrchestrationValidator;
import com.latticeengines.datacloudapi.engine.orchestration.service.OrchestrationService;
import com.latticeengines.domain.exposed.datacloud.manage.Orchestration;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.orchestration.OrchestrationConfig;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("orchestrationService")
public class OrchestrationServiceImpl implements OrchestrationService {

    private static Logger log = LoggerFactory.getLogger(OrchestrationServiceImpl.class);

    @Autowired
    private OrchestrationEntityMgr orchestrationEntityMgr;

    @Autowired
    private OrchestrationProgressEntityMgr orchestrationProgressEntityMgr;

    @Autowired
    private OrchestrationProgressService orchestrationProgressService;

    @Autowired
    private OrchestrationValidator orchestrationValidator;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected YarnClient yarnClient;

    @Autowired
    private PropDataTenantService propDataTenantService;

    @Override
    public List<OrchestrationProgress> scan(String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        killFailedProgresses();
        triggerAll();
        return kickoffAll();
    }

    private void killFailedProgresses() {
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("Status", ProgressStatus.PROCESSING);
        List<OrchestrationProgress> progresses = orchestrationProgressEntityMgr.findProgressesByField(fields, null);
        for (OrchestrationProgress progress : progresses) {
            ApplicationId appId = ConverterUtils.toApplicationId(progress.getApplicationId());
            try {
                ApplicationReport report = YarnUtils.getApplicationReport(yarnClient, appId);
                if (report == null || report.getYarnApplicationState() == null
                        || report.getYarnApplicationState().equals(YarnApplicationState.FAILED)
                        || report.getYarnApplicationState().equals(YarnApplicationState.KILLED)) {
                    progress = orchestrationProgressService.updateProgress(progress).status(ProgressStatus.FAILED)
                            .message("Found application status to be FAILED or KILLED in the scan").commit(true);
                    log.info("Killed progress: " + progress.toString());
                }
            } catch (YarnException | IOException e) {
                log.error("Failed to track application status for " + progress.getApplicationId() + ". Error: "
                        + e.toString());
                if (e.getMessage().contains("doesn't exist in the timeline store")) {
                    progress = orchestrationProgressService.updateProgress(progress).status(ProgressStatus.FAILED)
                            .message("Failed to track application status in the scan").commit(true);
                    log.info("Killed progress: " + progress.toString());
                }
            }
        }
    }

    private void triggerAll() {
        List<Orchestration> orchs = orchestrationEntityMgr.findAll();
        List<OrchestrationProgress> progresses = new ArrayList<>();
        for (Orchestration orch : orchs) {
            List<String> triggeredVersions = new ArrayList<>();
            if (orchestrationValidator.isTriggered(orch, triggeredVersions)) {
                log.info(String.format("triggered orchestration: %s, triggered versions: %s", orch.toString(),
                        String.join(",", triggeredVersions)));
                progresses.addAll(orchestrationProgressService.createDraftProgresses(orch, triggeredVersions));
            }
        }
        orchestrationProgressEntityMgr.saveProgresses(progresses);
    }

    private List<OrchestrationProgress> kickoffAll() {
        List<OrchestrationProgress> progresses = orchestrationProgressService.findProgressesToKickoff();
        if (progresses == null) {
            return new ArrayList<>();
        }
        List<OrchestrationProgress> submitted = new ArrayList<>();
        Boolean serviceTenantBootstrapped = false;
        for (OrchestrationProgress progress : progresses) {
            try {
                if (!serviceTenantBootstrapped) {
                    propDataTenantService.bootstrapServiceTenant();
                    serviceTenantBootstrapped = true;
                }
                ApplicationId applicationId = submitWorkflow(progress);
                progress = orchestrationProgressService.updateSubmittedProgress(progress, applicationId.toString());
                submitted.add(progress);
                log.info(String.format("Submitted workflow for progress [%s]. ApplicationID = %s", progress.toString(),
                        applicationId.toString()));
            } catch (Exception e) {
                log.error("Failed to submit workflow for progress " + progress, e);
            }
        }
        return submitted;
    }

    private ApplicationId submitWorkflow(OrchestrationProgress progress) {
        Orchestration orch = progress.getOrchestration();
        OrchestrationConfig config = orch.getConfig();
        return new OrchestrationWorkflowSubmitter() //
                .workflowProxy(workflowProxy) //
                .orchestration(orch).orchestrationProgress(progress).orchestrationConfig(config)
                .submit();
    }

}

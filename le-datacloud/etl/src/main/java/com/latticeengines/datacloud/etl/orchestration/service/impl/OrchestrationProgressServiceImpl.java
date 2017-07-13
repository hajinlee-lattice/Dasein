package com.latticeengines.datacloud.etl.orchestration.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.core.util.PropDataConstants;
import com.latticeengines.datacloud.etl.orchestration.entitymgr.OrchestrationProgressEntityMgr;
import com.latticeengines.datacloud.etl.orchestration.service.OrchestrationProgressService;
import com.latticeengines.domain.exposed.datacloud.manage.Orchestration;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngine;

@Component("orchestrationProgressService")
public class OrchestrationProgressServiceImpl implements OrchestrationProgressService {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(OrchestrationProgressServiceImpl.class);

    @Autowired
    private OrchestrationProgressEntityMgr orchestrationProgressEntityMgr;


    @Override
    public OrchestrationProgressUpdater updateProgress(OrchestrationProgress progress) {
        progress = orchestrationProgressEntityMgr.findProgress(progress);
        return new OrchestrationProgressUpdater(progress);
    }

    @Override
    public List<OrchestrationProgress> createDraftProgresses(Orchestration orch, List<String> triggeredVersions) {
        List<OrchestrationProgress> progresses = new ArrayList<>();
        for (String version : triggeredVersions) {
            OrchestrationProgress progress = new OrchestrationProgress();
            progress.setHdfsPod(HdfsPodContext.getHdfsPodId());
            progress.setOrchestration(orch);
            progress.setVersion(version);
            progress.setStartTime(new Date());
            progress.setLatestUpdateTime(new Date());
            progress.setRetries(0);
            progress.setStatus(ProgressStatus.NEW);
            progress.setTriggeredBy(PropDataConstants.SCAN_SUBMITTER);
            progresses.add(progress);
        }
        return progresses;
    }

    @Override
    public List<OrchestrationProgress> findProgressesToKickoff() {
        return orchestrationProgressEntityMgr.findProgressesToKickoff();
    }

    @Override
    public OrchestrationProgress updateSubmittedProgress(OrchestrationProgress progress, String applicationId) {
        progress.setApplicationId(applicationId);
        progress.setLatestUpdateTime(new Date());
        progress.setStartTime(new Date());
        if (progress.getStatus() == ProgressStatus.FAILED) {
            progress.setRetries(progress.getRetries() + 1);
        }
        progress.setStatus(ProgressStatus.PROCESSING);
        progress.setCurrentStage(null);
        orchestrationProgressEntityMgr.saveProgress(progress);
        return progress;
    }

    public class OrchestrationProgressUpdater {
        private final OrchestrationProgress progress;

        OrchestrationProgressUpdater(OrchestrationProgress progress) {
            this.progress = progress;
        }

        public OrchestrationProgressUpdater status(ProgressStatus status) {
            this.progress.setStatus(status);
            return this;
        }

        public OrchestrationProgressUpdater currentStage(DataCloudEngine stage) {
            this.progress.setCurrentStage(stage);
            return this;
        }

        public OrchestrationProgressUpdater message(String message) {
            this.progress.setMessage(message);
            return this;
        }

        public OrchestrationProgress commit(boolean persistent) {
            progress.setLatestUpdateTime(new Date());
            if (persistent) {
                return orchestrationProgressEntityMgr.saveProgress(progress);
            } else {
                return progress;
            }
        }
    }

}

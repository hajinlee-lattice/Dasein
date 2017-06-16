package com.latticeengines.datacloud.etl.orchestration.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.orchestration.entitymgr.OrchestrationProgressEntityMgr;
import com.latticeengines.datacloud.etl.orchestration.service.OrchestrationValidator;
import com.latticeengines.datacloud.etl.service.DataCloudEngineService;
import com.latticeengines.domain.exposed.datacloud.manage.Orchestration;
import com.latticeengines.domain.exposed.datacloud.manage.OrchestrationProgress;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngine;
import com.latticeengines.domain.exposed.datacloud.orchestration.ExternalTriggerConfig;
import com.latticeengines.domain.exposed.datacloud.orchestration.PredefinedScheduleConfig;

@Component("orchestrationValidator")
public class OrchestrationValidatorImpl implements OrchestrationValidator {
    private static final Log log = LogFactory.getLog(OrchestrationValidatorImpl.class);

    @Autowired
    private OrchestrationProgressEntityMgr orchestrationProgressEntityMgr;

    @Autowired
    private List<DataCloudEngineService> engineServices;

    private Map<DataCloudEngine, DataCloudEngineService> serviceMap;

    @PostConstruct
    private void postConstruct() {
        serviceMap = new HashMap<>();
        for (DataCloudEngineService service : engineServices) {
            serviceMap.put(service.getEngine(), service);
        }
    }

    @Override
    public boolean isTriggered(Orchestration orch, List<String> triggeredVersions) {
        if (!orch.isSchedularEnabled()) {
            return false;
        }
        if (triggeredVersions == null) {
            triggeredVersions = new ArrayList<>();
        }
        if (orch.getConfig() instanceof PredefinedScheduleConfig) {
            PredefinedScheduleConfig config = (PredefinedScheduleConfig) orch.getConfig();
            if (StringUtils.isBlank(config.getCronExpression())) {
                return false;
            } else { // TODO: Add cron expression checking, cron expression to version conversion
                return true;
            }
        }
        if (orch.getConfig() instanceof ExternalTriggerConfig) {
            ExternalTriggerConfig config = (ExternalTriggerConfig) orch.getConfig();
            DataCloudEngineService service = serviceMap.get(config.getEngine());
            if (service == null) {
                throw new UnsupportedOperationException(String
                        .format("Not support to trigger orchestration from %s engine", config.getEngine().name()));
            }
            switch (config.getStrategy()) {
            case LATEST_VERSION:
                String currentVersion = service.findCurrentVersion(config.getEngineName());
                if (StringUtils.isEmpty(currentVersion)) {
                    return false;
                }
                if (!isDuplicateVersion(orch.getName(), currentVersion)) {
                    triggeredVersions.add(currentVersion);
                    return true;
                } else {
                    return false;
                }
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported external trigger strategy %s", config.getStrategy().name()));
            }

        }
        throw new UnsupportedOperationException(
                String.format("Unsupported orchestration config: %s", orch.getConfig().toString()));
    }

    @Override
    public List<OrchestrationProgress> cleanupDuplicateProgresses(List<OrchestrationProgress> progresses) {
        Iterator<OrchestrationProgress> iter = progresses.iterator();
        while (iter.hasNext()) {
            OrchestrationProgress progress = iter.next();
            if (isDuplicateVersion(progress.getOrchestration().getName(), progress.getVersion())) {
                iter.remove();
                log.info("Duplicate progress is ignored: " + progress.toString());
            }
        }
        return progresses;
    }

    private boolean isDuplicateVersion(String orchName, String version) {
        return orchestrationProgressEntityMgr.isDuplicateVersion(orchName, version);
    }

}

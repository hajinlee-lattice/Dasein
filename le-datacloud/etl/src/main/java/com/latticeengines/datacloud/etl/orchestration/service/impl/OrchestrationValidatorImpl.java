package com.latticeengines.datacloud.etl.orchestration.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.collections.CollectionUtils;
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
import com.latticeengines.domain.exposed.datacloud.orchestration.EngineTriggeredConfig;
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
        if (orch.getConfig() instanceof EngineTriggeredConfig) {
            EngineTriggeredConfig config = (EngineTriggeredConfig) orch.getConfig();
            DataCloudEngineService service = serviceMap.get(config.getEngine());
            if (service == null) {
                throw new UnsupportedOperationException(String
                        .format("Not support to trigger orchestration from %s engine", config.getEngine().name()));
            }
            // TODO: Should check all versions?
            String currentVersion = service.findCurrentVersion(config.getEngineName());
            if (StringUtils.isEmpty(currentVersion)) {
                return false;
            }
            if (!isDuplicateVersion(orch, currentVersion)) {
                triggeredVersions.add(currentVersion);
                return true;
            } else {
                return false;
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
            if (isDuplicateVersion(progress.getOrchestration(), progress.getVersion())) {
                iter.remove();
                log.info("Duplicate progress is ignored: " + progress.toString());
            }
        }
        return progresses;
    }

    private boolean isDuplicateVersion(Orchestration orch, String version) {
        Map<String, Object> fields = new HashMap<>();
        fields.put("Orchestration", orch.getPid());
        fields.put("Version", version);
        List<OrchestrationProgress> progresses = orchestrationProgressEntityMgr.findProgressesByField(fields, null);
        return !CollectionUtils.isEmpty(progresses);
    }

}

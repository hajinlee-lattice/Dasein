package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.activity.JourneyStage;

public interface JourneyStageService {

    JourneyStage findByPid(String customerSpace, Long pid);

    JourneyStage findByStageName(String customerSpace, String stageName);

    List<JourneyStage> findByTenant(String customerSpace);

    JourneyStage createOrUpdate(String customerSpace, JourneyStage journeyStage);

    void createDefaultJourneyStages(String customerSpace);

    void delete(String customerSpace, JourneyStage journeyStage);
}

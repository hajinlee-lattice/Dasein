package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.AtlasScheduling;

public interface AtlasSchedulingService {

    void createOrUpdateSchedulingByType(String customerSpace, String cronExpression, AtlasScheduling.ScheduleType type);

    AtlasScheduling findSchedulingByType(String customerSpace, AtlasScheduling.ScheduleType type);

    List<AtlasScheduling> findAllByType(AtlasScheduling.ScheduleType type);

    void updateExportScheduling(AtlasScheduling atlasScheduling);
}

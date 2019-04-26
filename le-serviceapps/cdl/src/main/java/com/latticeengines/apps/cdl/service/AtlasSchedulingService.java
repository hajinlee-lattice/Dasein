package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.AtlasScheduling;

public interface AtlasSchedulingService {

    void createOrUpdateExportScheduling(String customerSpace, String cronExpression);

    AtlasScheduling findSchedulingByType(String customerSpace, AtlasScheduling.ScheduleType type);
}

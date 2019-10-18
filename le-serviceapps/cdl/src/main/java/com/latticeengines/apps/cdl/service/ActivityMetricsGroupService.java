package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;

public interface ActivityMetricsGroupService {
    ActivityMetricsGroup findByPid(String customerSpace, Long pid);

    List<ActivityMetricsGroup> setupDefaultWebVisitProfile(String customerSpace, String streamName);
}

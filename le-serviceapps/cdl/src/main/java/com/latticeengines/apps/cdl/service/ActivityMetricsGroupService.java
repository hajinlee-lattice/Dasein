package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;

public interface ActivityMetricsGroupService {

    ActivityMetricsGroup findByPid(String customerSpace, Long pid);

    List<ActivityMetricsGroup> findByStream(String customerSpace, AtlasStream stream);

    List<ActivityMetricsGroup> setupDefaultWebVisitProfile(String customerSpace, String streamName);
}

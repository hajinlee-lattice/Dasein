package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.CreateActivityMetricsGroupRequest;

public interface ActivityMetricsGroupService {

    ActivityMetricsGroup findByPid(String customerSpace, Long pid);

    List<ActivityMetricsGroup> findByStream(String customerSpace, AtlasStream stream);

    List<ActivityMetricsGroup> setupDefaultWebVisitGroups(String customerSpace, String streamName);

    ActivityMetricsGroup setUpDefaultOpportunityGroup(String customerSpace, String streamName);

    List<ActivityMetricsGroup> setupDefaultMarketingGroups(String customerSpace, String streamName);

    List<ActivityMetricsGroup> setupDefaultDnbIntentGroups(String customerSpace, String streamName);

    boolean createCustomizedGroup(String customerSpace, CreateActivityMetricsGroupRequest request);
}

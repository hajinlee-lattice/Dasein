package com.latticeengines.app.exposed.service;

import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.query.DataPage;

public interface ActivityTimelineService {

    DataPage getAccountActivities(String accountId, String timelinePeriod, String backPeriod,
            Set<AtlasStream.StreamType> streamTypes, Map<String, String> orgInfo);

    DataPage getContactActivities(String accountId, String contactId, String timelinePeriod,
            Set<AtlasStream.StreamType> streamTypes, Map<String, String> orgInfo);

    Map<String, Integer> getActivityTimelineMetrics(String accountId, String timelinePeriod,
            Map<String, String> orgInfo);

}

package com.latticeengines.app.exposed.service;

import java.util.Map;

import com.latticeengines.domain.exposed.query.DataPage;

public interface ActivityTimelineService {

    DataPage getCompleteTimelineActivities(String accountId, String timelinePeriod, String backPeriod,
            Map<String, String> orgInfo);

    DataPage getContactActivities(String accountId, String contactId, String timelinePeriod,
            Map<String, String> orgInfo);

    Map<String, Integer> getActivityTimelineMetrics(String accountId, String timelinePeriod,
            Map<String, String> orgInfo);

}

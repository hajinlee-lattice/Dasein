package com.latticeengines.app.exposed.service;

import java.util.Map;

import com.latticeengines.domain.exposed.query.DataPage;

public interface ActivityTimelineService {

    DataPage getAccountActivities(String accountId, String timelinePeriod, Map<String, String> orgInfo);

    DataPage getContactActivities(String accountId, String contactId, String timelinePeriod,
            Map<String, String> orgInfo);
}

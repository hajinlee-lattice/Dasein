package com.latticeengines.app.exposed.service;

import java.util.Map;

import com.latticeengines.domain.exposed.cdl.activity.AlertCategory;
import com.latticeengines.domain.exposed.query.DataPage;

public interface ActivityAlertsService {
    DataPage findActivityAlertsByAccountAndCategory(String customerSpace, String accountId, AlertCategory category,
            int max, Map<String, String> orgInfo);
}

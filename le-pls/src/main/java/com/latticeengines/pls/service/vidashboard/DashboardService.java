package com.latticeengines.pls.service.vidashboard;

import com.latticeengines.domain.exposed.cdl.dashboard.DashboardResponse;

public interface DashboardService {

    void create(String customerSpace, String esIndexName);

    DashboardResponse getDashboardList(String customerSpace);
}

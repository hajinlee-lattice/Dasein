package com.latticeengines.pls.service.vidashboard;

import com.latticeengines.domain.exposed.cdl.dashboard.DashboardResponse;

public interface DashboardService {

    DashboardResponse getDashboardList(String customerSpace);
}

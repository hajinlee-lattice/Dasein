package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.pls.RatingEngineDashboard;

public interface RatingEngineDashboardService {

    RatingEngineDashboard getRatingsDashboard(String customerSpace, String ratingEngineId);
}

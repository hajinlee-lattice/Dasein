package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.RatingEngineDashboard;

public interface RatingEngineDashboardService {

    RatingEngineDashboard getRatingsDashboard(String ratingEngineId);
}

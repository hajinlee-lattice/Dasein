package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.RatingsCountRequest;
import com.latticeengines.domain.exposed.pls.RatingsCountResponse;

public interface RatingCoverageService {
    RatingsCountResponse getCoverageInfo(RatingsCountRequest request);
}

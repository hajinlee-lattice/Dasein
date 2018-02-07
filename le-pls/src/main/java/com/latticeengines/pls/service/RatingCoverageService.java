package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.RatingsCountRequest;
import com.latticeengines.domain.exposed.pls.RatingsCountResponse;

public interface RatingCoverageService {

    String SEGMENT_IDS_ERROR_MAP_KEY = "processSegmentIdsErrorMap";

    RatingsCountResponse getCoverageInfo(RatingsCountRequest request);

}

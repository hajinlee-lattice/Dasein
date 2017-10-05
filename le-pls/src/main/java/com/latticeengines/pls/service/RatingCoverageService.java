package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.RatingsCountRequest;
import com.latticeengines.domain.exposed.pls.RatingsCountResponse;

public interface RatingCoverageService {

    static final String SEGMENT_ID_SINGLE_RULES_ERROR_MAP_KEY = "processSegmentIdSingleRulesErrorMap";

    static final String SEGMENT_ID_MODEL_RULES_ERROR_MAP_KEY = "processSegmentIdModelRulesErrorMap";

    static final String SEGMENT_IDS_ERROR_MAP_KEY = "processSegmentIdsErrorMap";

    static final String RATING_IDS_ERROR_MAP_KEY = "processRatingIdsErrorMap";

    RatingsCountResponse getCoverageInfo(RatingsCountRequest request);

}

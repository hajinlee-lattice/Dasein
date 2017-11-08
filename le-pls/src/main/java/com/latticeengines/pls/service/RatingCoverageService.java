package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.CoverageInfo;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingsCountRequest;
import com.latticeengines.domain.exposed.pls.RatingsCountResponse;

public interface RatingCoverageService {

    String SEGMENT_ID_SINGLE_RULES_ERROR_MAP_KEY = "processSegmentIdSingleRulesErrorMap";

    String SEGMENT_ID_MODEL_RULES_ERROR_MAP_KEY = "processSegmentIdModelRulesErrorMap";

    String SEGMENT_IDS_ERROR_MAP_KEY = "processSegmentIdsErrorMap";

    String RATING_IDS_ERROR_MAP_KEY = "processRatingIdsErrorMap";

    RatingsCountResponse getCoverageInfo(RatingsCountRequest request);

    CoverageInfo getCoverageInfo(RatingEngine ratingEngine);

}

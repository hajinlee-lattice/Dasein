package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.ratings.coverage.ProductsCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingEnginesCoverageResponse;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountRequest;
import com.latticeengines.domain.exposed.ratings.coverage.RatingsCountResponse;

public interface RatingCoverageService {

    String SEGMENT_ID_SINGLE_RULES_ERROR_MAP_KEY = "processSegmentIdSingleRulesErrorMap";

    String SEGMENT_ID_MODEL_RULES_ERROR_MAP_KEY = "processSegmentIdModelRulesErrorMap";

    String SEGMENT_IDS_ERROR_MAP_KEY = "processSegmentIdsErrorMap";

    String RATING_IDS_ERROR_MAP_KEY = "processRatingIdsErrorMap";

    String RATING_ID_LOOKUP_COL_PAIR_ERROR_MAP_KEY = "processRatingIdLookupColumnPairs";

    RatingsCountResponse getCoverageInfo(String customeSpace, RatingsCountRequest request);

    RatingEnginesCoverageResponse getRatingCoveragesForSegment(String customerSpace, String segmentName,
                                                               RatingEnginesCoverageRequest ratingCoverageRequest);

    RatingEnginesCoverageResponse getProductCoveragesForSegment(String customerSpace,
            ProductsCoverageRequest productsCoverageRequest, Integer purchasedBeforePeriod);

}

package com.latticeengines.dataplatform.service;

import com.latticeengines.domain.exposed.modelreview.ModelReviewData;

public interface ModelReviewService {

    ModelReviewData getReviewData(String modelId);

}

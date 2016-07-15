package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.modelreview.ModelReviewData;

public interface ModelReviewService {

    ModelReviewData getReviewData(String customerSpace, String modelId, String eventTableName);

}

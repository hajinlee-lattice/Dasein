package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;

public interface RatingEntityPreviewService {

    DataPage getEntityPreview(RatingEngine ratingEngine, long offset, long maximum, BusinessEntity entityType,
            String sortBy, boolean descending, String bucketFieldName, List<String> lookupFieldNames,
            boolean restrictNotNullSalesforceId, String freeFormTextSearch, List<String> selectedBuckets);

}

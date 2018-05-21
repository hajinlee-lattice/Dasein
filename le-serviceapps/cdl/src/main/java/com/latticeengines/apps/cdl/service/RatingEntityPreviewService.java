package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;

public interface RatingEntityPreviewService {

    DataPage getEntityPreview(RatingEngine ratingEngine, long offset, long maximum, BusinessEntity entityType,
            Boolean restrictNotNullSalesforceId, List<String> selectedBuckets, String lookupIdColumn);

    DataPage getEntityPreview(RatingEngine ratingEngine, long offset, long maximum, BusinessEntity entityType,
            String sortBy, boolean descending, String bucketFieldName, List<String> lookupFieldNames,
            Boolean restrictNotNullSalesforceId, String freeFormTextSearch, List<String> selectedBuckets,
            String lookupIdColumn);

    Long getEntityPreviewCount(RatingEngine ratingEngine, BusinessEntity entityType,
            Boolean restrictNotNullSalesforceId, String freeFormTextSearch, List<String> selectedBuckets,
            String lookupIdColumn);
}

package com.latticeengines.testframework.exposed.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.RatingBucketName;

public interface CDLTestDataService {

    void populateData(String tenantId);
    
    void mockRatingTableWithSingleEngine(String tenantId, String engineId, //
            Map<RatingBucketName, Long> coverage);

    void mockRatingTable(String tenantId, List<String> engineIds, //
            Map<String, Map<RatingBucketName, Long>> coverages);

}

package com.latticeengines.testframework.exposed.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.pls.BucketMetadata;

public interface CDLTestDataService {

    void populateData(String tenantId);

    void mockRatingTableWithSingleEngine(String tenantId, String engineId, //
            List<BucketMetadata> coverage);

    void mockRatingTable(String tenantId, List<String> engineIds, //
            Map<String, List<BucketMetadata>> coverages);

}

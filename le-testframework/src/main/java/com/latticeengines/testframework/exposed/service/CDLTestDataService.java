package com.latticeengines.testframework.exposed.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.pls.BucketMetadata;

public interface CDLTestDataService {

    void populateData(String tenantId, int version);

    void populateMetadata(String tenantId, int version);

    void mockRatingTableWithSingleEngine(String tenantId, String engineId, //
                                         List<BucketMetadata> coverage);

    void mockRatingTable(String tenantId, List<String> engineIds, //
                         Map<String, List<BucketMetadata>> modelRatingBuckets, boolean uploadRatingTable);

    void mockRatingTableWithSingleEngine(String tenantId, String engineId, List<BucketMetadata> coverage, boolean uploadRatingTable);

}

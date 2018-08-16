package com.latticeengines.proxy.exposed.lp;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketedScoreSummary;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;
import com.latticeengines.domain.exposed.serviceapps.lp.UpdateBucketMetadataRequest;

public interface BucketedScoreProxy {

    void createABCDBuckets(String customerSpace, CreateBucketMetadataRequest request);

    List<BucketMetadata> updateABCDBuckets(String customerSpace, UpdateBucketMetadataRequest request);

    Map<Long, List<BucketMetadata>> getABCDBucketsByModelGuid(String customerSpace, String modelGuid);

    Map<Long, List<BucketMetadata>> getABCDBucketsByEngineId(String customerSpace, String engineId);

    List<BucketMetadata> getLatestABCDBucketsByModelGuid(String customerSpace, String modelGuid);

    List<BucketMetadata> getLatestABCDBucketsByEngineId(String customerSpace, String engineId);

    List<BucketMetadata> getAllBucketsByEngineId(String customerSpace, String engineId);

    BucketedScoreSummary getBucketedScoreSummary(String customerSpace, String modelGuid);

    BucketedScoreSummary createOrUpdateBucketedScoreSummary(String customerSpace, String modelGuid,
            BucketedScoreSummary summary);

}

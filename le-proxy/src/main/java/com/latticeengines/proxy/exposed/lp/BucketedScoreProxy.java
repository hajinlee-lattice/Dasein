package com.latticeengines.proxy.exposed.lp;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;

public interface BucketedScoreProxy {

    void createABCDBuckets(String customerSpace, CreateBucketMetadataRequest request);

    Map<Long, List<BucketMetadata>> getABCDBucketsByModelGuid(String customerSpace, String modelGuid);

    List<BucketMetadata> getLatestABCDBucketsByModelGuid(String customerSpace, String modelGuid);

    List<BucketMetadata> getABCDBucketsByEngineId(String customerSpace, String engineId);

}

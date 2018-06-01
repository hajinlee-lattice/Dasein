package com.latticeengines.aws.elasticache;

import java.util.List;

public interface ElasticCacheService {

    List<String> getDistributedCacheNodeAddresses();

    String getPrimaryEndpointAddress();

}

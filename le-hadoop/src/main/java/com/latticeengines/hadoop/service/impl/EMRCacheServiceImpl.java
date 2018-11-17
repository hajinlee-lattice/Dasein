package com.latticeengines.hadoop.service.impl;

import javax.inject.Inject;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.hadoop.service.EMRCacheService;

@Service("emrCacheService")
public class EMRCacheServiceImpl implements EMRCacheService {

    @Inject
    private EMRService emrService;

    @Cacheable(cacheNames = CacheName.Constants.EMRClusterCacheName, key = "T(java.lang.String).format(\"%s|masterip\", #clusterName)")
    public String getMasterIp(String clusterName) {
        return emrService.getMasterIp(clusterName);
    }

    @Cacheable(cacheNames = CacheName.Constants.EMRClusterCacheName, key = "T(java.lang.String).format(\"%s|encrypted\", #clusterName)")
    public Boolean isEncrypted(String clusterName) {
        return emrService.isEncrypted(clusterName);
    }

}

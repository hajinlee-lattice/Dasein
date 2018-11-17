package com.latticeengines.hadoop.service.impl;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Service;

import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.hadoop.service.EMRCacheService;

@Service("emrCacheService")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class EMRCacheServiceImpl implements EMRCacheService {

    @Inject
    private EMRService emrService;

    @Inject
    private EMRCacheServiceImpl _impl;

    @Value("${aws.emr.cluster}")
    private String clusterName;

    public String getMasterIp() {
        return _impl.getMasterIp(clusterName);
    }

    @Cacheable(cacheNames = CacheName.Constants.EMRClusterCacheName, key = "T(java.lang.String).format(\"%s|masterip\", #clusterName)")
    public String getMasterIp(String clusterName) {
        return emrService.getMasterIp(clusterName);
    }

    @Cacheable(cacheNames = CacheName.Constants.EMRClusterCacheName, key = "T(java.lang.String).format(\"%s|encrypted\", #clusterName)")
    public Boolean isEncrypted(String clusterName) {
        return emrService.isEncrypted(clusterName);
    }

}

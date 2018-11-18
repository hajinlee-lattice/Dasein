package com.latticeengines.hadoop.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
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

    private String mainClusterId;

    @Override
    public String getMasterIp() {
        return _impl.getMasterIp(clusterName);
    }

    @Override
    public String getClusterId() {
        if (StringUtils.isBlank(mainClusterId)) {
            synchronized (this) {
                if (StringUtils.isBlank(mainClusterId)) {
                    mainClusterId = _impl.getClusterId(clusterName);
                }
            }
        }
        return mainClusterId;
    }

    @Override
    public String getWebHdfsUrl() {
        return "http://" + getMasterIp() + ":50070/webhdfs/v1";
    }

    @Override
    @Cacheable(cacheNames = CacheName.Constants.EMRClusterCacheName, key = "T(java.lang.String).format(\"%s|clusterid\", #clusterName)")
    public String getClusterId(String clusterName) {
        return emrService.getClusterId(clusterName);
    }

    @Override
    @Cacheable(cacheNames = CacheName.Constants.EMRClusterCacheName, key = "T(java.lang.String).format(\"%s|masterip\", #clusterName)")
    public String getMasterIp(String clusterName) {
        String clusterId = _impl.getClusterId(clusterName);
        return emrService.getMasterIp(clusterId);
    }

    @Override
    @Cacheable(cacheNames = CacheName.Constants.EMRClusterCacheName, key = "T(java.lang.String).format(\"%s|encrypted\", #clusterName)")
    public Boolean isEncrypted(String clusterName) {
        return emrService.isEncrypted(clusterName);
    }

}

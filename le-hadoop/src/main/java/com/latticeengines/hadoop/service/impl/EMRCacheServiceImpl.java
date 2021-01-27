package com.latticeengines.hadoop.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Service;

import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.ConsulUtils;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;

@Service("emrCacheService")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class EMRCacheServiceImpl implements EMRCacheService {

    private static final Logger log = LoggerFactory.getLogger(EMRCacheServiceImpl.class);

    @Inject
    private EMRService emrService;

    @Inject
    private EMRCacheServiceImpl _impl;

    @Value("${aws.emr.cluster}")
    private String clusterName;

    @Value("${hadoop.consul.url}")
    private String consul;

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
    public String getLivyUrl() {
        return "http://" + getMasterIp() + ":8998";
    }

    @Override
    public String getClusterId(String clusterName) {
        return _impl.getClusterIdFromAWS(clusterName);
    }

    @Cacheable(cacheNames = CacheName.Constants.EMRClusterCacheName, key = "T(java.lang.String).format(\"%s|clusterid\", #clusterName)", unless = "#result == null")
    public String getClusterIdFromAWS(String clusterName) {
        String clusterId = "";
        try {
            if (StringUtils.isNotBlank(consul)) {
                clusterId = getClusterIdFromConsul(clusterName);
            }
        } catch (Exception e) {
            log.info("Failed to get cluster id for " + clusterName + " from consul.", e);
            clusterId = "";
        }
        if (StringUtils.isBlank(clusterId)) {
            try (PerformanceTimer timer = new PerformanceTimer("Got the cluster id for " + clusterName)) {
                log.info("Getting the cluster id for " + clusterName);
                clusterId = emrService.getClusterId(clusterName);
            }
        }
        return StringUtils.isNotBlank(clusterId) ? clusterId : null;
    }

    @Override
    @Cacheable(cacheNames = CacheName.Constants.EMRClusterCacheName, key = "T(java.lang.String).format(\"%s|masterip\", #clusterName)", unless = "#result == null")
    public String getMasterIp(String clusterName) {
        String masterIp = null;
        try {
            if (StringUtils.isNotBlank(consul)) {
                masterIp = getMasterIpFromConsul(clusterName);
            }
        } catch (Exception e) {
            log.info("Failed to get master ip for " + clusterName + " from consul.", e);
            masterIp = null;
        }
        if (StringUtils.isBlank(masterIp)) {
            try (PerformanceTimer timer = new PerformanceTimer("Got the master ip for " + clusterName)) {
                log.info("Getting the master ip for " + clusterName);
                String clusterId = getClusterId(clusterName);
                masterIp = emrService.getMasterIp(clusterId);
            }
        }
        return StringUtils.isNotBlank(masterIp) ? masterIp : null;
    }

    private String getClusterIdFromConsul(String clusterName) {
        String consulKey = "emr/" + clusterName + "/ClusterId";
        return ConsulUtils.getValueFromConsul(consul, consulKey);
    }

    private String getMasterIpFromConsul(String clusterName) {
        String consulKey = "emr/" + clusterName + "/MasterIp";
        return ConsulUtils.getValueFromConsul(consul, consulKey);
    }
}

package com.latticeengines.hadoop.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.hadoop.service.EMRCacheService;

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
    public String getClusterId(String clusterName) {
        RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(5);
        return retryTemplate.execute(context -> _impl.getClusterIdFromAWS(clusterName));
    }

    @Cacheable(cacheNames = CacheName.Constants.EMRClusterCacheName, key = "T(java.lang.String).format(\"%s|clusterid\", #clusterName)", sync=true)
    public String getClusterIdFromAWS(String clusterName) {
        String clusterId = "";
        try {
            if (StringUtils.isNotBlank(consul)) {
                RetryTemplate retry = RetryUtils.getRetryTemplate(5);
                clusterId = retry.execute(context -> getClusterIdFromConsul(clusterName));
            }
        } catch (Exception e) {
            log.info("Failed to get cluster id from consul.", e);
            clusterId = "";
        }
        if (StringUtils.isBlank(clusterId)) {
            clusterId = emrService.getClusterId(clusterName);
        }
        return clusterId;
    }

    @Override
    @Cacheable(cacheNames = CacheName.Constants.EMRClusterCacheName, key = "T(java.lang.String).format(\"%s|masterip\", #clusterName)")
    public String getMasterIp(String clusterName) {
        String clusterId = getClusterId(clusterName);
        return emrService.getMasterIp(clusterId);
    }

    @Override
    @Cacheable(cacheNames = CacheName.Constants.EMRClusterCacheName, key = "T(java.lang.String).format(\"%s|encrypted\", #clusterName)")
    public Boolean isEncrypted(String clusterName) {
        String clusterId = getClusterId(clusterName);
        return emrService.isEncrypted(clusterId);
    }

    private String getClusterIdFromConsul(String clusterName) {
        String clusterId = "";
        String consul = "http://internal-consul-1214146536.us-east-1.elb.amazonaws.com:8500";
        String emrKvUrl = consul + "/v1/kv/emr/" + clusterName + "/ClusterId";
        RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
        JsonNode json = restTemplate.getForObject(emrKvUrl, JsonNode.class);
        if (json != null && json.size() >= 1) {
            String data = json.get(0).get("Value").asText();
            clusterId = new String(Base64Utils.decodeBase64(data));
        }
        return clusterId;
    }

}

package com.latticeengines.hadoop.service.impl;

import java.util.Collections;

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
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.common.exposed.util.Base64Utils;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
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
        RetryTemplate retryTemplate = RetryUtils.getRetryTemplate(5);
        return retryTemplate.execute(context -> _impl.getClusterIdFromAWS(clusterName));
    }

    @Cacheable(cacheNames = CacheName.Constants.EMRClusterCacheName, key = "T(java.lang.String).format(\"%s|clusterid\", #clusterName)", unless="#result == null")
    public String getClusterIdFromAWS(String clusterName) {
        String clusterId = "";
        try {
            if (StringUtils.isNotBlank(consul)) {
                clusterId = getClusterIdFromConsul(clusterName);
            }
        } catch (Exception e) {
            log.info("Failed to get cluster id for " + clusterName + "from consul.", e);
            clusterId = "";
        }
        if (StringUtils.isBlank(clusterId)) {
            clusterId = emrService.getClusterId(clusterName);
        }
        return clusterId;
    }

    @Override
    @Cacheable(cacheNames = CacheName.Constants.EMRClusterCacheName, key = "T(java.lang.String).format(\"%s|masterip\", #clusterName)", unless="#result == null")
    public String getMasterIp(String clusterName) {
        String masterIp = "";
        try {
            if (StringUtils.isNotBlank(consul)) {
                masterIp = getMasterIpFromConsul(clusterName);
            }
        } catch (Exception e) {
            log.info("Failed to get master ip for " + clusterName + " from consul.", e);
            masterIp = "";
        }
        if (StringUtils.isBlank(masterIp)) {
            String clusterId = getClusterId(clusterName);
            masterIp = emrService.getMasterIp(clusterId);
        }
        return masterIp;
    }

    private String getClusterIdFromConsul(String clusterName) {
        String consulKey = "emr/" + clusterName + "/ClusterId";
        return getValueFromConsul(consulKey);
    }

    private String getMasterIpFromConsul(String clusterName) {
        String consulKey = "emr/" + clusterName + "/MasterIp";
        return getValueFromConsul(consulKey);
    }

    private String getValueFromConsul(String key) {
        String kvUrl = consul + "/v1/kv/" + key;
        RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
        RetryTemplate retry = RetryUtils.getRetryTemplate(5, null, //
                Collections.singleton(HttpClientErrorException.NotFound.class));
        try {
            JsonNode json = retry.execute(context -> restTemplate.getForObject(kvUrl, JsonNode.class));
            if (json != null && json.size() >= 1) {
                String data = json.get(0).get("Value").asText();
                return new String(Base64Utils.decodeBase64(data));
            }
        } catch (HttpClientErrorException.NotFound e) {
            // key not exists
            return null;
        }
        return null;
    }

}

package com.latticeengines.yarn.exposed.service.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.yarn.client.CommandYarnClient;
import org.springframework.yarn.client.YarnClientFactoryBean;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HeaderRequestInterceptor;
import com.latticeengines.domain.exposed.yarn.ClusterMetrics;
import com.latticeengines.hadoop.bean.HadoopConfigurationUtils;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@Service("emrEnvService")
public class EMREnvServiceImpl implements EMREnvService {

    private static final Logger log = LoggerFactory.getLogger(EMREnvServiceImpl.class);

    @Inject
    private EMRCacheService emrCacheService;

    @Inject
    private ApplicationContext appCtx;

    @Value("${aws.default.access.key}")
    protected String awsKey;

    @Value("${aws.default.secret.key.encrypted}")
    protected String awsSecret;

    @Value("${dataplatform.python.conda.env}")
    private String condaEnv;

    @Value("${dataplatform.python.conda.env.ambari}")
    private String condaEnvAmbari;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${dataplatform.queue.scheme}")
    private String ambariQueueScheme;

    private RestTemplate restTemplate;

    private ObjectMapper om = new ObjectMapper();

    @Override
    public String getLatticeCondaEnv() {
        if (Boolean.TRUE.equals(useEmr)) {
            return condaEnv;
        } else {
            return condaEnvAmbari;
        }
    }

    @Override
    public String getYarnQueueScheme() {
        if (Boolean.TRUE.equals(useEmr)) {
            return "default";
        } else {
            return ambariQueueScheme;
        }
    }

    @Override
    public YarnConfiguration getYarnConfiguration(String emrCluster) {
        String masterIp;
        if (StringUtils.isNotBlank(emrCluster)) {
            masterIp = emrCacheService.getMasterIp(emrCluster);
        } else {
            masterIp = emrCacheService.getMasterIp();
        }

        Properties properties = HadoopConfigurationUtils.loadPropsFromResource("emr.properties", //
                masterIp, awsKey, awsSecret);
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        properties.forEach((k, v) -> yarnConfiguration.set((String) k, (String) v));
        yarnConfiguration.set("hadoop.rpc.protection", "privacy");
        String fs = yarnConfiguration.get("fs.defaultFS");
        log.info(String.format("Created a YarnConfiguration (%d): %s", //
                System.identityHashCode(yarnConfiguration), fs));
        return yarnConfiguration;
    }

    @Override
    public CommandYarnClient getSpringYarnClient(String emrCluster) {
        YarnConfiguration yarnConfiguration = getYarnConfiguration(emrCluster);
        YarnClientFactoryBean factoryBean = (YarnClientFactoryBean) appCtx.getBean("&sprintYarnClient");
        factoryBean.setConfiguration(yarnConfiguration);
        factoryBean.setAppName(emrCluster);
        try {
            return (CommandYarnClient) factoryBean.getObject();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create spring yarn client", e);
        }
    }

    // remember to close the client, use try {}
    @Override
    public YarnClient getYarnClient(String emrCluster) {
        YarnConfiguration yarnConfiguration = getYarnConfiguration(emrCluster);
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        return yarnClient;
    }

    @Override
    public ClusterMetrics getClusterMetrics(String emrCluster) {
        String masterIp = emrCacheService.getMasterIp(emrCluster);
        String metricsUrl = String.format("http://%s:8088/ws/v1/cluster/metrics", masterIp);
        RestTemplate restTemplate = getRestTemplate();
        JsonNode json = restTemplate.getForObject(metricsUrl, JsonNode.class);
        try {
            return om.treeToValue(json.get("clusterMetrics"), ClusterMetrics.class);
        } catch (IOException e) {
            throw new RuntimeException("Cannot parse cluster metrics", e);
        }
    }

    private RestTemplate getRestTemplate() {
        if (restTemplate == null) {
            synchronized (this) {
                if (restTemplate == null) {
                    restTemplate = new RestTemplate();
                    restTemplate.setInterceptors(Collections.singletonList(//
                            new HeaderRequestInterceptor(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)));
                }
            }
        }
        return restTemplate;
    }
}

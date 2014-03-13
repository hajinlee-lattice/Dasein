package com.latticeengines.dataplatform.exposed.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.dataplatform.exposed.service.YarnService;

@Component("yarnService")
public class YarnServiceImpl implements YarnService {

    private RestTemplate rmRestTemplate = new RestTemplate();

    @Autowired
    private Configuration yarnConfiguration;

    private String getResourceManagerEndpoint() {
        String rmHostPort = yarnConfiguration.get("yarn.resourcemanager.webapp.address");
        return "http://" + rmHostPort + "/ws/v1/cluster";
    }

    @Override
    public SchedulerTypeInfo getSchedulerInfo() {
        String rmRestEndpointBaseUrl = getResourceManagerEndpoint();
        return rmRestTemplate.getForObject(rmRestEndpointBaseUrl + "/scheduler", SchedulerTypeInfo.class);
    }

    @Override
    public AppsInfo getApplications(String queryString) {
        String rmRestEndpointBaseUrl = getResourceManagerEndpoint();
        if (queryString == null || queryString.length() == 0) {
            return rmRestTemplate.getForObject(rmRestEndpointBaseUrl + "/apps", AppsInfo.class);
        } else {
            return rmRestTemplate.getForObject(rmRestEndpointBaseUrl + "/apps?" + queryString, AppsInfo.class);
        }
    }

    @Override
    public AppInfo getApplication(String appId) {
        String rmRestEndpointBaseUrl = getResourceManagerEndpoint();
        return rmRestTemplate.getForObject(rmRestEndpointBaseUrl + "/apps/" + appId, AppInfo.class);
    }

}

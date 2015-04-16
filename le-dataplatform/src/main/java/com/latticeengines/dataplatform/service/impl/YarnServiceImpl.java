package com.latticeengines.dataplatform.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.google.common.collect.ComparisonChain;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.scheduler.exposed.fairscheduler.LedpQueueAssigner;

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
    public List<AppInfo> getPreemptedApps() {
        AppsInfo appsInfo = getApplications("states=FAILED");
        ArrayList<AppInfo> appInfos = appsInfo.getApps();

        List<AppInfo> preemptedApps = new ArrayList<AppInfo>();
        for (AppInfo appInfo : appInfos) {
            if (!appInfo.getQueue().contains(LedpQueueAssigner.PRIORITY)) {
                // Disregard non-Tahoe apps (ex: Ambari service diagnostic apps)
                continue;
            }
            String diagnostics = appInfo.getNote();
            if (YarnUtils.isPrempted(diagnostics)) {
                preemptedApps.add(appInfo);
            }
        }
        Collections.sort(preemptedApps, new Comparator<AppInfo>() {

            @Override
            public int compare(AppInfo o1, AppInfo o2) {
                String q1 = o1.getQueue();
                String q2 = o2.getQueue();
                String wordBeingSearched = LedpQueueAssigner.PRIORITY;
                int wordBeingSearchedLength = wordBeingSearched.length();
                int priorityIndex = q1.indexOf(wordBeingSearched) + wordBeingSearchedLength;
                int priorityNextIndex = priorityIndex + 1;
                Integer p1 = Integer.parseInt(q1.substring(priorityIndex, priorityNextIndex));
                Integer p2 = Integer.parseInt(q2.substring(priorityIndex, priorityNextIndex));

                return ComparisonChain.start().compare(p1, p2).compare(o1.getStartTime(), o2.getStartTime()).result();
            }

        });

        return preemptedApps;
    }

    @Override
    public AppInfo getApplication(String appId) {
        String rmRestEndpointBaseUrl = getResourceManagerEndpoint();
        return rmRestTemplate.getForObject(rmRestEndpointBaseUrl + "/apps/" + appId, AppInfo.class);
    }

}

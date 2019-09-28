package com.latticeengines.yarn.exposed.service.impl;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedulerTypeInfo;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.client.RestTemplate;

import com.google.common.collect.ComparisonChain;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.service.YarnService;

@Component("yarnService")
public class YarnServiceImpl implements YarnService {

    @Resource(name = "yarnConfiguration")
    private Configuration yarnConfiguration;

    private RestTemplate rmRestTemplate = new RestTemplate();

    private YarnClient yarnClient;

    @PostConstruct
    public void postConstruct() {
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> yarnClient.stop()));
    }

    private String getResourceManagerEndpoint() {
        if (yarnConfiguration.getBoolean(YarnConfiguration.RM_HA_ENABLED, false)) {
            String currentId = yarnConfiguration.get(YarnConfiguration.RM_HA_ID);
            String rmHostPort = yarnConfiguration.get(YarnConfiguration.RM_WEBAPP_ADDRESS + "." + currentId);
            return "http://" + rmHostPort + "/ws/v1/cluster";
        } else {
            String rmHostPort = yarnConfiguration.get(YarnConfiguration.RM_WEBAPP_ADDRESS);
            return "http://" + rmHostPort + "/ws/v1/cluster";
        }
    }

    /**
     * Use apache yarn client or remove this functionality
     */
    @Deprecated
    @Override
    public List<ApplicationReport> getApplications(GetApplicationsRequest request) {
        try {
            return yarnClient.getApplications(request.getApplicationStates());
        } catch (IOException | YarnException e) {
            throw new RuntimeException("Failed to get application reports", e);
        }
    }

    @Override
    public List<ApplicationReport> getRunningApplications(GetApplicationsRequest request) {
        request.setApplicationStates(EnumSet.of(YarnApplicationState.NEW, YarnApplicationState.NEW_SAVING,
                YarnApplicationState.SUBMITTED, YarnApplicationState.ACCEPTED, YarnApplicationState.RUNNING));
        return getApplications(request);
    }

    @Override
    public List<ApplicationReport> getPreemptedApps() {
        GetApplicationsRequest request = GetApplicationsRequest.newInstance(EnumSet.of(YarnApplicationState.FAILED));
        List<ApplicationReport> appReports = getApplications(request);

        List<ApplicationReport> preemptedApps = new ArrayList<ApplicationReport>();
        for (ApplicationReport appReport : appReports) {
            if (!appReport.getQueue().contains(LedpQueueAssigner.PRIORITY)) {
                // Disregard non-Tahoe apps (ex: Ambari service diagnostic apps)
                continue;
            }
            String diagnostics = appReport.getDiagnostics();
            if (YarnUtils.isPrempted(diagnostics)) {
                preemptedApps.add(appReport);
            }
        }
        Collections.sort(preemptedApps, new Comparator<ApplicationReport>() {

            @Override
            public int compare(ApplicationReport o1, ApplicationReport o2) {
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
    public SchedulerTypeInfo getSchedulerInfo() {
        String rmRestEndpointBaseUrl = getResourceManagerEndpoint();
        synchronized (yarnConfiguration) {
            try {
                return rmRestTemplate.getForObject(rmRestEndpointBaseUrl + "/scheduler", SchedulerTypeInfo.class);
            } catch (Exception e) {
                rmRestEndpointBaseUrl = performFailover();
                return rmRestTemplate.getForObject(rmRestEndpointBaseUrl + "/scheduler", SchedulerTypeInfo.class);
            }
        }
    }

    @Override
    public CapacitySchedulerInfo getCapacitySchedulerInfo() {
        SchedulerTypeInfo schedulerTypeInfo = this.getSchedulerInfo();

        Field field = ReflectionUtils.findField(SchedulerTypeInfo.class, "schedulerInfo");
        field.setAccessible(true);
        CapacitySchedulerInfo capacitySchedulerInfo = null;
        try {
            capacitySchedulerInfo = (CapacitySchedulerInfo) field.get(schedulerTypeInfo);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_12012, e);
        }
        return capacitySchedulerInfo;
    }

    @Override
    public ApplicationReport getApplication(String appId) {
        try {
            return yarnClient.getApplicationReport(ApplicationIdUtils.toApplicationIdObj(appId));
        } catch (IOException | YarnException e) {
            throw new RuntimeException("Failed to get application report", e);
        }
    }

    private String performFailover() {
        Collection<String> rmIds = HAUtil.getRMHAIds(yarnConfiguration);
        String[] rmServiceIds = rmIds.toArray(new String[rmIds.size()]);
        int currentIndex = 0;
        String currentHAId = yarnConfiguration.get(YarnConfiguration.RM_HA_ID);
        for (int i = 0; i < rmServiceIds.length; i++) {
            if (currentHAId.equals(rmServiceIds[i])) {
                currentIndex = i;
                break;
            }
        }
        currentIndex = (currentIndex + 1) % rmServiceIds.length;
        yarnConfiguration.set(YarnConfiguration.RM_HA_ID, rmServiceIds[currentIndex]);
        String rmHostPort = yarnConfiguration
                .get(YarnConfiguration.RM_WEBAPP_ADDRESS + "." + rmServiceIds[currentIndex]);
        String address = yarnConfiguration.get(YarnConfiguration.RM_ADDRESS + "." + rmServiceIds[currentIndex]);
        String webappAddress = yarnConfiguration
                .get(YarnConfiguration.RM_WEBAPP_ADDRESS + "." + rmServiceIds[currentIndex]);
        String schedulerAddress = yarnConfiguration
                .get(YarnConfiguration.RM_SCHEDULER_ADDRESS + "." + rmServiceIds[currentIndex]);
        yarnConfiguration.set(YarnConfiguration.RM_ADDRESS, address);
        yarnConfiguration.set(YarnConfiguration.RM_WEBAPP_ADDRESS, webappAddress);
        yarnConfiguration.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, schedulerAddress);

        return "http://" + rmHostPort + "/ws/v1/cluster";
    }

}

package com.latticeengines.dataplatform.service.impl.watchdog;

import java.util.List;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.stereotype.Component;

@Component("convertSuccessfulJobsToPMML")
public class ConvertSuccessfulJobsToPMML extends WatchdogPlugin {

    public ConvertSuccessfulJobsToPMML() {
        register(this);
    }

    @Override
    public void run(JobExecutionContext context) throws JobExecutionException {
        AppsInfo appsInfo = getYarnService().getApplications("states=FINISHED");
        
        List<AppInfo> apps = appsInfo.getApps();
        
        for (AppInfo app : apps) {
            if (!app.getApplicationType().equals("YARN")) {
                continue;
            }
        }
    }
}

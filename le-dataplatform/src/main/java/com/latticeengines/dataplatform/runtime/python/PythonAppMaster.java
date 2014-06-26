package com.latticeengines.dataplatform.runtime.python;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.am.ContainerLauncherInterceptor;
import org.springframework.yarn.am.StaticEventingAppmaster;
import org.springframework.yarn.am.container.AbstractLauncher;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.client.yarn.AppMasterProperty;
import com.latticeengines.dataplatform.client.yarn.ContainerProperty;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.runtime.metric.LedpMetricsMgr;

public class PythonAppMaster extends StaticEventingAppmaster implements ContainerLauncherInterceptor {

    private final static Log log = LogFactory.getLog(PythonAppMaster.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private YarnService yarnService;

    private LedpMetricsMgr ledpMetricsMgr = null;

    private String pythonContainerId;

    @Override
    protected void onInit() throws Exception {
        log.info("Initializing application.");
        setEnvironment(System.getenv());
        super.onInit();
        if (getLauncher() instanceof AbstractLauncher) {
            ((AbstractLauncher) getLauncher()).addInterceptor(this);
        }
        registerAppmaster();
        String appAttemptId = getApplicationAttemptId().toString();
        final String appId = getApplicationId(appAttemptId);

        ledpMetricsMgr = LedpMetricsMgr.getInstance(appAttemptId);
        final long appStartTime = System.currentTimeMillis();
        ledpMetricsMgr.setAppStartTime(appStartTime);

        log.info("Application id = " + getApplicationId(appId));

        new Thread(new Runnable() {

            @Override
            public void run() {
            	AppInfo appInfo = yarnService.getApplication(appId);
                String queue = appInfo.getQueue();
                if (queue == null) {
                    throw new LedpException(LedpCode.LEDP_12006);
                }

                ledpMetricsMgr.setQueue(queue);
                ledpMetricsMgr.start();
                
                long appSubmissionTime = appInfo.getStartTime();
                log.info("App start latency = " + (appStartTime - appSubmissionTime));
                ledpMetricsMgr.setAppSubmissionTime(appSubmissionTime);
            }

        }).start();
    }

    private String getApplicationId(String appAttemptId) {
        String[] tokens = appAttemptId.split("_");
        return "application_" + tokens[1] + "_" + tokens[2];
    }

    @Override
    public void setParameters(Properties parameters) {
        if (parameters == null) {
            return;
        }            
        for (Map.Entry<Object, Object> parameter : parameters.entrySet()) {
            log.info("Key = " + parameter.getKey().toString() + " Value = " + parameter.getValue().toString());
        }
        super.setParameters(parameters);
        
        String priority = parameters.getProperty(ContainerProperty.PRIORITY.name());
        if (priority == null) {
            throw new LedpException(LedpCode.LEDP_12000);
        }
        String customer = parameters.getProperty(AppMasterProperty.CUSTOMER.name());
        if (customer == null) {
            throw new LedpException(LedpCode.LEDP_12007);
        }
        ledpMetricsMgr.setPriority(priority);
        ledpMetricsMgr.setCustomer(customer);;
    }

    @Override
    public ContainerLaunchContext preLaunch(Container container, ContainerLaunchContext context) {
        pythonContainerId = container.getId().toString();
        log.info("Container id = " + pythonContainerId);
        return context;
    }

    @Override
    protected void onContainerLaunched(Container container) {
        String containerId = container.getId().toString();
        log.info("Launching container id = " + containerId + ".");
        ledpMetricsMgr.setContainerId(containerId);
        ledpMetricsMgr.setContainerLaunchTime(System.currentTimeMillis());
        super.onContainerLaunched(container);
    }

    private void cleanupJobDir() {
        String dir = "/app/dataplatform/" + getParameters().getProperty(ContainerProperty.JOBDIR.name());
        try {
            HdfsUtils.rmdir(yarnConfiguration, dir);
        } catch (Exception e) {
            log.warn("Could not delete job dir " + dir + " due to exception:\n" + ExceptionUtils.getStackTrace(e));
        }
    }

    @Override
    protected void onContainerCompleted(ContainerStatus status) { 	
        if (status.getExitStatus() == ContainerExitStatus.SUCCESS) {
            log.info("Container id = " + status.getContainerId().toString() + " completed.");
            ledpMetricsMgr.setContainerEndTime(System.currentTimeMillis());
            // immediately publish 
            ledpMetricsMgr.publishMetricsNow();
        }

        if (status.getExitStatus() != ContainerExitStatus.PREEMPTED) {

        } else {
            log.info("Printing out the status to find the reason: " + status.getExitStatus());
        }     
        super.onContainerCompleted(status);
    }

    @Override
    protected boolean onContainerFailed(ContainerStatus status) {
        String containerId = status.getContainerId().toString();

        if (status.getExitStatus() == ContainerExitStatus.PREEMPTED) {
            ledpMetricsMgr.incrementNumberPreemptions();
            // immediately publish 
            ledpMetricsMgr.publishMetricsNow();
            try {
                Thread.sleep(5000L);
                log.info("Container " + containerId + " preempted. Reallocating.");
            } catch (InterruptedException e) {
                log.error(e);
            }
            
            getAllocator().allocateContainers(1);
            return true;
        }
        log.info(status.getDiagnostics());

        if (status.getExitStatus() == ContainerExitStatus.ABORTED) {
            if (!containerId.equals(pythonContainerId)) {
                log.info("Releasing container " + containerId + ". Ignoring abort error.");
                return true;
            } else {
                return false;
            }
        }

        return false;
    }

    @Override
    protected void doStop() {
        super.doStop();
        cleanupJobDir();
        ledpMetricsMgr.setAppEndTime(System.currentTimeMillis());
        // immediately publish 
        ledpMetricsMgr.publishMetricsNow();
    }

}

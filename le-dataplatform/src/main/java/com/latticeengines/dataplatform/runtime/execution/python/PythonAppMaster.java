package com.latticeengines.dataplatform.runtime.execution.python;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.am.ContainerLauncherInterceptor;
import org.springframework.yarn.am.StaticEventingAppmaster;
import org.springframework.yarn.am.container.AbstractLauncher;

import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.runtime.metric.LedpMetricsMgr;
import com.latticeengines.dataplatform.util.HdfsHelper;
import com.latticeengines.dataplatform.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.yarn.client.ContainerProperty;

public class PythonAppMaster extends StaticEventingAppmaster implements ContainerLauncherInterceptor {

    private final static Log log = LogFactory.getLog(PythonAppMaster.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private YarnService yarnService;

    private LedpMetricsMgr ledpMetricsMgr = null;

    private String pythonContainerId;

    private String appId;

    @Override
    protected void onInit() throws Exception {
        log.info("Initializing application.");
        super.onInit();
        if (getLauncher() instanceof AbstractLauncher) {
            ((AbstractLauncher) getLauncher()).addInterceptor(this);
        }
        String appAttemptId = getApplicationAttemptId().toString();
        appId = getApplicationId(appAttemptId);

        ledpMetricsMgr = LedpMetricsMgr.getInstance(appAttemptId);
        final long appStartTime = System.currentTimeMillis();
        ledpMetricsMgr.setAppStartTime(appStartTime);

        log.info("Application id = " + getApplicationId(appId));

        new Thread(new Runnable() {

            @Override
            public void run() {
                long appSubmissionTime = yarnService.getApplication(appId).getStartTime();
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
        String queue = parameters.getProperty(AppMasterProperty.QUEUE.name());
        String appName = yarnService.getApplication(appId).getName();
        String customer = appName.split("~")[0];
        ledpMetricsMgr.setPriority(priority);
        ledpMetricsMgr.setQueue(queue);
        ledpMetricsMgr.setCustomer(customer);
        ledpMetricsMgr.start();
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
            HdfsHelper.rmdir(yarnConfiguration, dir);
        } catch (Exception e) {
            log.warn("Could not delete job dir " + dir + " due to exception:\n" + ExceptionUtils.getStackTrace(e));
        }
    }

    @Override
    protected void onContainerCompleted(ContainerStatus status) {
        if (status.getExitStatus() == ContainerExitStatus.SUCCESS) {
            log.info("Container id = " + status.getContainerId().toString() + " completed.");
            ledpMetricsMgr.setContainerEndTime(System.currentTimeMillis());
        }

        if (status.getExitStatus() != ContainerExitStatus.PREEMPTED) {
            cleanupJobDir();
        }
        super.onContainerCompleted(status);
    }

    @Override
    protected boolean onContainerFailed(ContainerStatus status) {
        String containerId = status.getContainerId().toString();

        if (status.getExitStatus() == ContainerExitStatus.PREEMPTED) {
            ledpMetricsMgr.incrementNumberPreemptions();
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
        ledpMetricsMgr.setAppEndTime(System.currentTimeMillis());
        // ledpMetricsMgr.resetContainerElapsedTimeForGanglia();
    }

}

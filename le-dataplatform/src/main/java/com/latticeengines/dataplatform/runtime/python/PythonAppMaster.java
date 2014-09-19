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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.yarn.am.ContainerLauncherInterceptor;
import org.springframework.yarn.am.StaticEventingAppmaster;
import org.springframework.yarn.am.container.AbstractLauncher;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.yarn.ProgressMonitor;
import com.latticeengines.common.exposed.yarn.RuntimeConfig;
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

    private ProgressMonitor monitor;

    private String priority;

    private String customer;

    @Value("${dataplatform.yarn.job.basedir}")
    private String hdfsJobBaseDir;

    @Override
    protected void onInit() throws Exception {
        log.info("Initializing application.");
        super.onInit();
        if (getLauncher() instanceof AbstractLauncher) {
            ((AbstractLauncher) getLauncher()).addInterceptor(this);
        }
        monitor = new ProgressMonitor(super.getAllocator());
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

        priority = parameters.getProperty(ContainerProperty.PRIORITY.name());
        if (priority == null) {
            throw new LedpException(LedpCode.LEDP_12000);
        }
        customer = parameters.getProperty(AppMasterProperty.CUSTOMER.name());
        if (customer == null) {
            throw new LedpException(LedpCode.LEDP_12007);
        }

        setRuntimeConfig(parameters);
    }

    @Override
    public void submitApplication() {
        super.submitApplication();
        String appAttemptId = getApplicationAttemptId().toString();
        final String appId = getApplicationId(appAttemptId);

        ledpMetricsMgr = LedpMetricsMgr.getInstance(appAttemptId);
        ledpMetricsMgr.setPriority(priority);
        ledpMetricsMgr.setCustomer(customer);
        final long appStartTime = System.currentTimeMillis();
        ledpMetricsMgr.setAppStartTime(appStartTime);

        log.info("Application submitted with Application id = " + getApplicationId(appId));

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

    @Override
    public ContainerLaunchContext preLaunch(Container container, ContainerLaunchContext context) {
        pythonContainerId = container.getId().toString();
        log.info("Container id = " + pythonContainerId);
        // Start monitoring process
        monitor.start();

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

    @Override
    protected void onContainerCompleted(ContainerStatus status) {
        if (status.getExitStatus() == ContainerExitStatus.SUCCESS) {
            log.info("Container id = " + status.getContainerId().toString() + " completed.");
            ledpMetricsMgr.setContainerEndTime(System.currentTimeMillis());
            // Immediately publish
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
            // Immediately publish
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
        // Immediately publish
        ledpMetricsMgr.publishMetricsNow();
        // Shut down monitor
        monitor.stop();
    }

    private String getApplicationId(String appAttemptId) {
        String[] tokens = appAttemptId.split("_");
        return "application_" + tokens[1] + "_" + tokens[2];
    }

    private void setRuntimeConfig(Properties parameters) {
        // Sets runtime host and port needed by python
        String configPath = hdfsJobBaseDir + "/" + parameters.getProperty(ContainerProperty.JOBDIR.name()) + "/"
                + parameters.getProperty(ContainerProperty.RUNTIME_CONFIG.name());
        RuntimeConfig runtimeConfig = new RuntimeConfig(configPath, yarnConfiguration);
        runtimeConfig.addProperties("host", monitor.getHost());
        runtimeConfig.addProperties("port", Integer.toString(monitor.getPort()));
        runtimeConfig.writeToHdfs();
        log.info("Writing runtime host: " + monitor.getHost() + " port: " + monitor.getPort());
    }

    private void cleanupJobDir() {
        String dir = hdfsJobBaseDir + "/" + getParameters().getProperty(ContainerProperty.JOBDIR.name());
        try {
            HdfsUtils.rmdir(yarnConfiguration, dir);
        } catch (Exception e) {
            log.warn("Could not delete job dir " + dir + " due to exception:\n" + ExceptionUtils.getStackTrace(e));
        }

    }
}

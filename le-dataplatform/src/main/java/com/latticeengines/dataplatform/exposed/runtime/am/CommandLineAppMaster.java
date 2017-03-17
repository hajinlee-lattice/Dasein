package com.latticeengines.dataplatform.exposed.runtime.am;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.yarn.am.AppmasterRmTemplate;
import org.springframework.yarn.am.ContainerLauncherInterceptor;
import org.springframework.yarn.am.StaticEventingAppmaster;
import org.springframework.yarn.am.container.AbstractLauncher;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.yarn.ProgressMonitor;
import com.latticeengines.common.exposed.yarn.RuntimeConfig;
import com.latticeengines.dataplatform.exposed.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class CommandLineAppMaster extends StaticEventingAppmaster implements ContainerLauncherInterceptor {

    private final static Log log = LogFactory.getLog(CommandLineAppMaster.class);

    @Autowired
    private Configuration yarnConfiguration;

    private ProgressMonitor monitor;

    private String containerId;

    private String customer;

    private String priority;

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
        try {
            AppmasterRmTemplate rmTemplate = (AppmasterRmTemplate) getTemplate();
            try {
                rmTemplate.afterPropertiesSet();
            } catch (Exception e) {
                log.error("AppmasterRmTemplate refresh properties failed.");
            }
            super.submitApplication();
        } catch (Exception e) {
            if (getConfiguration().getBoolean(YarnConfiguration.RM_HA_ENABLED, false)) {
                log.info("Retry submit application.");
                //performFailover();
                //super.submitApplication();
            } else {
                throw e;
            }
        }
        final String appId = getApplicationAttemptId().getApplicationId().toString();

        log.info("Application submitted with Application id = " + appId);

    }

    @Override
    public ContainerLaunchContext preLaunch(Container container, ContainerLaunchContext context) {
        logFilesInLocalFilesystem();
        containerId = container.getId().toString();
        log.info("Container id = " + containerId);
        // Start monitoring process
        monitor.start();

        return context;
    }

    @Override
    protected void onContainerLaunched(Container container) {
        String containerId = container.getId().toString();
        log.info("Launching container id = " + containerId + ".");
        super.onContainerLaunched(container);
    }

    @Override
    protected void onContainerCompleted(ContainerStatus status) {
        if (status.getExitStatus() == ContainerExitStatus.SUCCESS) {
            log.info("Container id = " + status.getContainerId().toString() + " completed.");
        }

        if (status.getExitStatus() == ContainerExitStatus.PREEMPTED) {
            log.info("Printing out the status to find the reason: " + status.getExitStatus());
        }
        super.onContainerCompleted(status);
    }

    @Override
    protected boolean onContainerFailed(ContainerStatus status) {
        String containerId = status.getContainerId().toString();

        if (status.getExitStatus() == ContainerExitStatus.PREEMPTED) {
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
            if (!containerId.equals(containerId)) {
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
        try {
            AppmasterRmTemplate rmTemplate = (AppmasterRmTemplate) getTemplate();
            try {
                rmTemplate.afterPropertiesSet();
            } catch (Exception e) {
                log.error("AppmasterRmTemplate refresh properties failed.");
            }
            super.doStop();
        } catch (Exception e) {
            if (getConfiguration().getBoolean(YarnConfiguration.RM_HA_ENABLED, false)) {
                log.info("Retry doStop.");
                //performFailover();
                //super.doStop();
            } else {
                throw e;
            }
        }
        cleanupJobDir();
        // Shut down monitor
        monitor.stop();
    }

    private void setRuntimeConfig(Properties parameters) {
        // Sets runtime host and port needed by container
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
            log.warn("Could not delete job dir " + dir + " due to exception:\n" + e.getMessage());
        }

    }

    private void logFilesInLocalFilesystem() {

        IOFileFilter filter = new IOFileFilter() {

            @Override
            public boolean accept(File file) {
                return true;
            }

            @Override
            public boolean accept(File dir, String name) {
                return true;
            }
        };
        Iterator<File> it = FileUtils.iterateFilesAndDirs(new File("."), filter, filter);
        while (it.hasNext()) {
            log.info(it.next().getAbsolutePath());
        }
    }

//    private void performFailover() {
//        Configuration conf = getConfiguration();
//        log.info(String.format("RM address before fail over: %s", conf.get(YarnConfiguration.RM_ADDRESS)));
//        Collection<String> rmIds = HAUtil.getRMHAIds(conf);
//        String[] rmServiceIds = rmIds.toArray(new String[rmIds.size()]);
//        int currentIndex = 0;
//        String currentHAId = conf.get(YarnConfiguration.RM_HA_ID);
//        for (int i = 0; i < rmServiceIds.length; i++) {
//            if (currentHAId.equals(rmServiceIds[i])) {
//                currentIndex = i;
//                break;
//            }
//        }
//        currentIndex = (currentIndex + 1) % rmServiceIds.length;
//        conf.set(YarnConfiguration.RM_HA_ID, rmServiceIds[currentIndex]);
//        String address = conf.get(YarnConfiguration.RM_ADDRESS + "." + rmServiceIds[currentIndex]);
//        String webappAddress = conf.get(YarnConfiguration.RM_WEBAPP_ADDRESS + "."
//                + rmServiceIds[currentIndex]);
//        String schedulerAddress = conf.get(YarnConfiguration.RM_SCHEDULER_ADDRESS + "."
//                + rmServiceIds[currentIndex]);
//        conf.set(YarnConfiguration.RM_ADDRESS, address);
//        conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, webappAddress);
//        conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, schedulerAddress);
//        setConfiguration(conf);
//        log.info(String.format("Fail over from %s to %s.", currentHAId, rmServiceIds[currentIndex]));
//        log.info(String.format("RM address after fail over: %s", conf.get(YarnConfiguration.RM_ADDRESS)));
//        AppmasterRmTemplate rmTemplate = (AppmasterRmTemplate) getTemplate();
//        try {
//            rmTemplate.afterPropertiesSet();
//        } catch (Exception e) {
//            log.error("AppmasterRmTemplate refresh properties failed.");
//        }
//    }
}

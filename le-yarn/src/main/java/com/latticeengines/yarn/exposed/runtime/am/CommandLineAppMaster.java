package com.latticeengines.yarn.exposed.runtime.am;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.yarn.am.ContainerLauncherInterceptor;
import org.springframework.yarn.am.StaticEventingAppmaster;
import org.springframework.yarn.am.allocate.DefaultContainerAllocator;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.yarn.ProgressMonitor;
import com.latticeengines.common.exposed.yarn.RuntimeConfig;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.yarn.exposed.client.AppMasterProperty;
import com.latticeengines.yarn.exposed.client.ContainerProperty;
import com.latticeengines.yarn.exposed.service.YarnService;

public class CommandLineAppMaster extends StaticEventingAppmaster
        implements ContainerLauncherInterceptor {

    private static final Logger log = LoggerFactory.getLogger(CommandLineAppMaster.class);

    @Autowired
    private YarnService yarnService;

    private Configuration yarnConfiguration;

    private ProgressMonitor monitor;

    private String customer;

    private String priority;

    @Value("${dataplatform.yarn.job.basedir}")
    private String hdfsJobBaseDir;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Override
    protected void onInit() throws Exception {
        log.info("Initializing application.");
        super.onInit();
        setTemplate(((DefaultContainerAllocator) getAllocator()).getRmTemplate());
        monitor = new ProgressMonitor(getAllocator());
        yarnConfiguration = super.getConfiguration();
        // TODO: YSong-M24 condition to be removed after cutting over to EMR
        if (Boolean.TRUE.equals(useEmr)) {
            logFilesInLocalFilesystem();
            log.info("Starting progress monitor ...");
            monitor.start();
        }
    }

    @Override
    public void setParameters(Properties parameters) {
        if (parameters == null) {
            return;
        }
        for (Map.Entry<Object, Object> parameter : parameters.entrySet()) {
            log.info("Key = " + parameter.getKey().toString() + " Value = "
                    + parameter.getValue().toString());
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
        final String appId = getApplicationAttemptId().getApplicationId().toString();

        log.info("Application submitted with Application id = " + appId);
        ApplicationReport appReport = yarnService.getApplication(appId);
        log.info("ApplicationReport: " + appReport);

    }

    @Override
    public ContainerLaunchContext preLaunch(Container container, ContainerLaunchContext context) {
        logFilesInLocalFilesystem();
        String containerId = container.getId().toString();
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
        String containerId = status.getContainerId().toString();
        super.onContainerCompleted(status);
        switch (status.getExitStatus()) {
            case ContainerExitStatus.SUCCESS:
                log.info("Container id = " + status.getContainerId().toString() + " completed.");
                break;
            case ContainerExitStatus.PREEMPTED:
                log.info("Printing out the status to find the reason: " + status.getExitStatus());
                break;
            case ContainerExitStatus.ABORTED:
                log.info("Releasing container " + containerId + ". Ignoring abort error.");
                setFinalApplicationStatus(FinalApplicationStatus.FAILED);
                notifyCompleted();
                break;
            default:
                break;
        }

    }

    @Override
    protected boolean onContainerFailed(ContainerStatus status) {
        String containerId = status.getContainerId().toString();

        if (status.getExitStatus() == ContainerExitStatus.PREEMPTED) {
            try {
                Thread.sleep(5000L);
                log.info("Container " + containerId + " preempted. Reallocating.");
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }

            getAllocator().allocateContainers(1);
            return true;
        }
        log.info(status.getDiagnostics());

        return false;
    }

    @Override
    protected void doStop() {
        super.doStop();
        cleanupJobDir();
        // Shut down monitor
        monitor.stop();
    }

    private void setRuntimeConfig(Properties parameters) {
        // Sets runtime host and port needed by container
        String configPath = hdfsJobBaseDir + "/"
                + parameters.getProperty(ContainerProperty.JOBDIR.name()) + "/"
                + parameters.getProperty(ContainerProperty.RUNTIME_CONFIG.name());
        RuntimeConfig runtimeConfig = new RuntimeConfig(configPath, yarnConfiguration);
        runtimeConfig.addProperties("host", monitor.getHost());
        runtimeConfig.addProperties("port", Integer.toString(monitor.getPort()));
        runtimeConfig.writeToHdfs();
        log.info("Writing runtime host: " + monitor.getHost() + " port: " + monitor.getPort());
    }

    private void cleanupJobDir() {
        String dir = hdfsJobBaseDir + "/"
                + getParameters().getProperty(ContainerProperty.JOBDIR.name());
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
}

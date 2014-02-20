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

import com.latticeengines.dataplatform.util.HdfsHelper;
import com.latticeengines.dataplatform.yarn.client.ContainerProperty;

public class PythonAppMaster extends StaticEventingAppmaster implements
        ContainerLauncherInterceptor {

    private final static Log log = LogFactory.getLog(PythonAppMaster.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    protected void onInit() throws Exception {
        log.info("Total memory used = " + Runtime.getRuntime().totalMemory());
        super.onInit();
        if (getLauncher() instanceof AbstractLauncher) {
            ((AbstractLauncher) getLauncher()).addInterceptor(this);
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
    }

    @Override
    public ContainerLaunchContext preLaunch(Container container,
            ContainerLaunchContext context) {
        log.info("Container id = " + container.getId().toString());
        return context;
    }

    private void cleanupJobDir() {
        String dir = "/app/dataplatform/"
                + getParameters().getProperty(ContainerProperty.JOBDIR.name());
        try {
            HdfsHelper.rmdir(yarnConfiguration, dir);
        } catch (Exception e) {
            log.warn("Could not delete job dir " + dir + " due to exception:\n"
                    + ExceptionUtils.getStackTrace(e));
        }
    }

    @Override
    protected void onContainerCompleted(ContainerStatus status) {
        if (status.getExitStatus() != ContainerExitStatus.PREEMPTED) {
            cleanupJobDir();
        }
        super.onContainerCompleted(status);
    }

    @Override
    protected boolean onContainerFailed(ContainerStatus status) {
        if (status.getExitStatus() == ContainerExitStatus.PREEMPTED) {
            try {
                Thread.sleep(15000L);
                log.info("Container " + status.getContainerId().toString()
                        + " preempted. Reallocating.");
            } catch (InterruptedException e) {
                log.error(e);
            }
            getAllocator().allocateContainers(1);
            return true;
        }
        log.info(status.getDiagnostics());

        if (status.getExitStatus() == ContainerExitStatus.ABORTED) {
            log.info("Container releasing "
                    + status.getContainerId().toString() + ".");
            log.info("Ignoring abort error.");
            return true;
        }
        log.info(status.getDiagnostics());
        return false;
    }

}

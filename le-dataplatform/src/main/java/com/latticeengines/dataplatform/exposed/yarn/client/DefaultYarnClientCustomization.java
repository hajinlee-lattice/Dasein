package com.latticeengines.dataplatform.exposed.yarn.client;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.springframework.yarn.fs.DefaultResourceLocalizer;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;
import org.springframework.yarn.fs.ResourceLocalizer;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.version.VersionManager;

public class DefaultYarnClientCustomization extends YarnClientCustomization {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(DefaultYarnClientCustomization.class);

    protected Configuration yarnConfiguration;

    protected VersionManager versionManager;

    private String hdfsJobBaseDir;

    private String webHdfs;

    public DefaultYarnClientCustomization(Configuration yarnConfiguration, VersionManager versionManager,
            String hdfsJobBaseDir, String webHdfs) {
        super();
        this.yarnConfiguration = yarnConfiguration;
        this.versionManager = versionManager;
        this.hdfsJobBaseDir = hdfsJobBaseDir;
        this.webHdfs = webHdfs;
    }

    @Override
    public ResourceLocalizer getResourceLocalizer(Properties containerProperties) {
        return new DefaultResourceLocalizer(yarnConfiguration, getHdfsEntries(containerProperties),
                getCopyEntries(containerProperties));
    }

    protected String getJobDir(Properties containerProperties) {
        return hdfsJobBaseDir + "/" + containerProperties.getProperty(ContainerProperty.JOBDIR.name());
    }

    @Override
    public Collection<CopyEntry> getCopyEntries(Properties containerProperties) {
        Collection<LocalResourcesFactoryBean.CopyEntry> copyEntries = new ArrayList<LocalResourcesFactoryBean.CopyEntry>();
        String containerLaunchContextFile = containerProperties.getProperty(ContainerProperty.APPMASTER_CONTEXT_FILE
                .name());
        copyEntries.add(new LocalResourcesFactoryBean.CopyEntry("file:" + containerLaunchContextFile,
                getJobDir(containerProperties), false));

        return copyEntries;
    }

    @Override
    public Collection<TransferEntry> getHdfsEntries(Properties containerProperties) {
        return getHdfsEntries(containerProperties, false);
    }

    protected Collection<TransferEntry> getHdfsEntries(Properties containerProperties, boolean excludeDataplatformLib) {
        Collection<LocalResourcesFactoryBean.TransferEntry> hdfsEntries = new ArrayList<LocalResourcesFactoryBean.TransferEntry>();
        hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                LocalResourceVisibility.PUBLIC, //
                String.format("/app/%s/dataplatform/*.properties", versionManager.getCurrentVersion()), //
                false));

        if (!excludeDataplatformLib) {
            hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                    LocalResourceVisibility.PUBLIC, //
                    String.format("/app/%s/dataplatform/lib/*.jar", versionManager.getCurrentVersion()), //
                    false));
        }
        hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                LocalResourceVisibility.PUBLIC, //
                getJobDir(containerProperties) + "/*", //
                false));
        return hdfsEntries;
    }

    @Override
    public String getClientId() {
        return "defaultYarnClient";
    }

    @Override
    public int getMemory(Properties properties) {
        String memory = properties.getProperty(AppMasterProperty.MEMORY.name(), "-1");
        return Integer.parseInt(memory);
    }

    @Override
    public int getPriority(Properties properties) {
        String priority = properties.getProperty(AppMasterProperty.PRIORITY.name(), "-1");
        return Integer.parseInt(priority);
    }

    @Override
    public String getQueue(Properties properties) {
        return properties.getProperty(AppMasterProperty.QUEUE.name());
    }

    @Override
    public int getVirtualcores(Properties properties) {
        String virtualCores = properties.getProperty(AppMasterProperty.VIRTUALCORES.name(), "-1");
        return Integer.parseInt(virtualCores);
    }

    @Override
    public String getContainerLauncherContextFile(Properties properties) {
        return "/default/dataplatform-default-appmaster-context.xml";
    }

    @VisibleForTesting
    String getXmxSetting(Properties containerProperties) {
        int minAllocationInMb = yarnConfiguration.getInt("yarn.scheduler.minimum-allocation-mb", -1);

        if (containerProperties != null) {
            String requestedMemoryStr = containerProperties.getProperty(ContainerProperty.MEMORY.name());

            if (requestedMemoryStr != null) {
                int requestedMemory = Integer.parseInt(requestedMemoryStr);
                if (requestedMemory > minAllocationInMb) {
                    minAllocationInMb = requestedMemory;
                }
            }
        }

        // -Xmx is just a heap setting, so must set the heap max value to be
        // less than the total requested/allocated memory.
        // This will ensure that garbage collection kicks in and reduces the
        // memory utilization. If we still run into an
        // OOM error, then that means the requested memory is really less than
        // what can be handled.
        String xmx = minAllocationInMb > 0 ? String.format("-Xmx%dm", minAllocationInMb - 512) : "-Xmx1024m";
        xmx += " -XX:PermSize=256m -XX:MaxPermSize=256m";
        return xmx;
    }

    @Override
    public List<String> getCommands(Properties containerProperties) {
        String containerLaunchContextFile = containerProperties.getProperty(ContainerProperty.APPMASTER_CONTEXT_FILE
                .name());
        if (containerLaunchContextFile == null) {
            throw new IllegalStateException("Property " + ContainerProperty.APPMASTER_CONTEXT_FILE + " does not exist.");
        }
        File contextFile = new File(containerLaunchContextFile);
        if (!contextFile.exists()) {
            throw new IllegalStateException("Container launcher context file " + containerLaunchContextFile
                    + " does not exist.");
        }
        String parameter = setupParameters(containerProperties);

        return Arrays.<String> asList(new String[] {
                "$JAVA_HOME/bin/java", //
                // "-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=4001,server=y,suspend=y",
                getXmxSetting(containerProperties),
                "org.springframework.yarn.am.CommandLineAppmasterRunnerForLocalContextFile", //
                contextFile.getName(), //
                "yarnAppmaster", //
                parameter, //
                "1><LOG_DIR>/Appmaster.stdout", //
                "2><LOG_DIR>/Appmaster.stderr" });
    }

    @Override
    public void beforeCreateLocalLauncherContextFile(Properties properties) {
    }

    @Override
    public void validate(Properties appMasterProperties, Properties containerProperties) {
    }

    @Override
    public void finalize(Properties appMasterProperties, Properties containerProperties) {
    }

    private String setupParameters(Properties containerProperties) {
        Properties prop = new Properties();
        for (Map.Entry<Object, Object> entry : containerProperties.entrySet()) {
            // Exclude METADATA_CONTENT to avoid nested properties
            if (!entry.getKey().toString().equals(ContainerProperty.METADATA_CONTENTS.name())) {
                prop.put(entry.getKey(), entry.getValue());
            }
        }
        String propStr = prop.toString();
        return propStr.substring(1, propStr.length() - 1).replaceAll(",", " ");
    }

    @Override
    public Map<String, String> setEnvironment(Map<String, String> environment, Properties containerProperties) {
        return environment;
    }

    public String getHdfsJobBaseDir() {
        return hdfsJobBaseDir;
    }

    public void setHdfsJobBaseDir(String hdfsJobBaseDir) {
        this.hdfsJobBaseDir = hdfsJobBaseDir;
    }

    public String getWebHdfs() {
        return webHdfs;
    }

    public void setWebHdfs(String webHdfs) {
        this.webHdfs = webHdfs;
    }

    public Configuration getConfiguration() {
        return yarnConfiguration;
    }
}

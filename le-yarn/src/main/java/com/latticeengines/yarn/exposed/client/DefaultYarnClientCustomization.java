package com.latticeengines.yarn.exposed.client;

import java.io.File;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.yarn.fs.DefaultResourceLocalizer;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;
import org.springframework.yarn.fs.ResourceLocalizer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.math.IntMath;
import com.latticeengines.camille.exposed.watchers.DebugGatewayWatcher;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JacocoUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

public class DefaultYarnClientCustomization extends YarnClientCustomization {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(DefaultYarnClientCustomization.class);

    protected VersionManager versionManager;

    protected String stackName;

    protected SoftwareLibraryService softwareLibraryService;

    private String hdfsJobBaseDir;

    private String webHdfs;

    public DefaultYarnClientCustomization(Configuration yarnConfiguration, //
            VersionManager versionManager, //
            String stackName, //
            SoftwareLibraryService softwareLibraryService, //
            String hdfsJobBaseDir, //
            String webHdfs) {
        super();
        this.yarnConfiguration = yarnConfiguration;
        this.versionManager = versionManager;
        this.stackName = stackName;
        this.softwareLibraryService = softwareLibraryService;
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
        String containerLaunchContextFile = containerProperties
                .getProperty(ContainerProperty.APPMASTER_CONTEXT_FILE.name());
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
                String.format("/app/%s/conf/latticeengines.properties",
                        versionManager.getCurrentVersionInStack(stackName)), //
                false));
        hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                LocalResourceVisibility.PUBLIC, //
                String.format("/app/%s/conf/log4j2-yarn.xml", versionManager.getCurrentVersionInStack(stackName)), //
                false));
        hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                LocalResourceVisibility.PUBLIC, //
                String.format("/app/%s/conf/log4j.properties", versionManager.getCurrentVersionInStack(stackName)), //
                false));
        if (JacocoUtils.needToLocalizeJacoco()) {
            hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                    LocalResourceVisibility.PUBLIC, //
                    "/apps/jacoco/jacocoagent.jar", //
                    false));
        }

        if (!excludeDataplatformLib) {
            hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                    LocalResourceVisibility.PUBLIC, //
                    String.format("/app/%s/dataplatform/lib/*.jar", versionManager.getCurrentVersionInStack(stackName)), //
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
        return "/default/yarn-default-appmaster-context.xml";
    }

    @VisibleForTesting
    String getXmxSetting(Properties appMasterProperties, boolean byFitting) {
        int minAllocationInMb = yarnConfiguration.getInt("yarn.scheduler.minimum-allocation-mb", -1);

        if (appMasterProperties != null) {
            String requestedMemoryStr = appMasterProperties.getProperty(AppMasterProperty.MEMORY.name());

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
        String xmx = String.format("-Xmx%dm",
                byFitting ? calHeapSizeByFitting(minAllocationInMb) : calHeapSizeByFixedPara(minAllocationInMb));
        String xms = String.format("-Xms%dm",
                byFitting ? calHeapSizeByFitting(minAllocationInMb) : calHeapSizeByFixedPara(minAllocationInMb));
        return xms + " " + xmx;
    }

    /**
     * Old behavior
     *
     * @param minAllocationInMb
     * @return
     */
    private int calHeapSizeByFixedPara(int minAllocationInMb) {
        if (minAllocationInMb < 1536) {
            return 1024;
        }
        return minAllocationInMb - 512;
    }

    /**
     * Updated behavior
     *
     * mem in (, 1536) -> heap = 1024
     * mem in [1536, 2048] -> heap = mem - 512
     * mem in (2048, 4096] -> heap = mem - 1024
     * mem in (4096, 8192] -> heap = mem - 1536
     * ... etc
     *
     * @param minAllocationInMb
     * @return
     */
    private int calHeapSizeByFitting(int minAllocationInMb) {
        if (minAllocationInMb < 1536) {
            return 1024;
        }
        return minAllocationInMb - 512 * (IntMath.log2(minAllocationInMb, RoundingMode.CEILING) - 10);
    }

    @Override
    public List<String> getCommands(Properties containerProperties, Properties appMasterProperties) {
        String containerLaunchContextFile = containerProperties
                .getProperty(ContainerProperty.APPMASTER_CONTEXT_FILE.name());
        if (containerLaunchContextFile == null) {
            throw new IllegalStateException(
                    "Property " + ContainerProperty.APPMASTER_CONTEXT_FILE + " does not exist.");
        }
        File contextFile = new File(containerLaunchContextFile);
        if (!contextFile.exists()) {
            throw new IllegalStateException(
                    "Container launcher context file " + containerLaunchContextFile + " does not exist.");
        }
        String parameter = setupParameters(containerProperties);

        List<String> commands = Arrays.asList( //
                // "-Xdebug -Xnoagent -Djava.compiler=NONE
                // -Xrunjdwp:transport=dt_socket,address=4023,server=y,suspend=y",
                "ls -alhR . > ${LOG_DIR}/directory.info", //
                "&&", //
                "cat launch_container.sh > ${LOG_DIR}/launch_container.sh", //
                "&&", //
                "$JAVA_HOME/bin/java", //
                getXmxSetting(appMasterProperties, true), //
                "${LE_LOG_OPTS}", //
                "${LE_CIPHER_OPTS}", //
                "${LE_JACOCO_OPT}",
                "${LE_TRUSTSTORE_OPT}", //
                "${LE_GC_OPTS}", //
                "org.springframework.yarn.am.CommandLineAppmasterRunnerForLocalContextFile", //
                contextFile.getName(), //
                "yarnAppmaster", //
                parameter, //
                "1>${LOG_DIR}/Appmaster.stdout", //
                "2>${LOG_DIR}/Appmaster.stderr");
        if (log.isDebugEnabled()) {
            log.debug("Commands send to YARN: " + commands);
        }
        return commands;
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

    @Override
    public void afterCreateLocalLauncherContextFile(Properties containerProperties) {
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
        String customer = containerProperties.getProperty(AppMasterProperty.CUSTOMER.name());
        String logLevel = "INFO";
        if (StringUtils.isNotBlank(customer)) {
            customer = CustomerSpace.shortenCustomerSpace(customer);
            if (DebugGatewayWatcher.hasPassport(customer)) {
                logLevel = "DEBUG";
                log.warn("Turn on YARN app DEBUG log for " + customer);
            }
        }

        environment.putIfAbsent("LOG_DIR", "<LOG_DIR>");
        environment.putIfAbsent("LE_LOG_OPTS", getLogOpts(logLevel));
        environment.putIfAbsent("LE_CIPHER_OPTS", CipherUtils.getSecretPropertyStr()); // secret
        environment.putIfAbsent("LE_JACOCO_OPT", getJacocoOpt(containerProperties));
        environment.putIfAbsent("LE_TRUSTSTORE_OPT", getTrustStoreOpt(containerProperties));
        environment.putIfAbsent("LE_GC_OPTS", getGcOpts());
        return environment;
    }

    private static String getGcOpts() {
        return StringUtils.join(Arrays.asList( //
                "-XX:+UseNUMA", //
                "-XX:+UseG1GC", //
                "-XX:+UseStringDeduplication", //
                "-XX:+PrintGCDateStamps",
                "-XX:+PrintGCTimeStamps", //
                "-XX:+PrintAdaptiveSizePolicy", //
                "-verbosegc", //
                "-Xloggc:${LOG_DIR}/gc.log" //
        ), " ");
    }

    private static String getLogOpts(String logLevel) {
        return StringUtils.join(Arrays.asList( //
                "-Dlog4j.debug", //
                "-Dlog4j.configuration=file:log4j.properties", //
                "-Dlog4j2.debug", //
                "-Dlog4j.configurationFile=log4j2-yarn.xml", //
                "-DLOG4J_LE_LEVEL=" + logLevel, //
                "-DLOG4J_DEBUG_DIR=${LOG_DIR}" //
        ), " ");
    }

    private static String getJacocoOpt(Properties properties) {
        if (properties.getProperty(ContainerProperty.JACOCO_AGENT_FILE.name()) != null
                && properties.getProperty(ContainerProperty.JACOCO_DEST_FILE.name()) != null) {
            return String.format("-javaagent:%s=destfile=%s,append=true,includes=com.latticeengines.*",
                    properties.getProperty(ContainerProperty.JACOCO_AGENT_FILE.name()),
                    properties.getProperty(ContainerProperty.JACOCO_DEST_FILE.name()));
        }
        return "";
    }

    private static String getTrustStoreOpt(Properties properties) {
        String trustStore = properties.getProperty(ContainerProperty.TRUST_STORE.name());
        if (StringUtils.isNotBlank(trustStore) && !trustStore.contains("#")) {
            return String.format("-Djavax.net.ssl.trustStore=%s", trustStore);
        }
        return "";
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

    public void setConfiguration(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }

    @Override
    public int getMaxAppAttempts(Properties appMasterProperties) {
        String maxAppAttempts = appMasterProperties.getProperty(AppMasterProperty.MAX_APP_ATTEMPTS.name(), "-1");
        if (maxAppAttempts.equals("-1")) {
            return yarnConfiguration.getInt("yarn.resourcemanager.am.max-attempts", 2);
        }
        return Integer.valueOf(maxAppAttempts);
    }

}

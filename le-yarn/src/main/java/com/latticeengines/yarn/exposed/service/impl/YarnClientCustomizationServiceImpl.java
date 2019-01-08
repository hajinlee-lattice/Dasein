package com.latticeengines.yarn.exposed.service.impl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;
import org.springframework.yarn.am.AppmasterConstants;
import org.springframework.yarn.client.CommandYarnClient;
import org.springframework.yarn.fs.ResourceLocalizer;

import com.latticeengines.yarn.exposed.client.AppMasterProperty;
import com.latticeengines.yarn.exposed.client.ContainerProperty;
import com.latticeengines.yarn.exposed.client.YarnClientCustomization;
import com.latticeengines.yarn.exposed.service.YarnClientCustomizationService;

@Component("yarnClientCustomizationService")
public class YarnClientCustomizationServiceImpl implements YarnClientCustomizationService {

    private static final Logger log = LoggerFactory.getLogger(YarnClientCustomizationServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${dataplatform.yarn.job.basedir}")
    private String hdfsJobBaseDir;

    @Value("${dataplatform.yarn.job.runtime.config}")
    private String runtimeConfig;

    @Autowired
    private JobServiceHelper jobServiceHelper;

    @Override
    public void addCustomizations(CommandYarnClient client, String clientName, Properties appMasterProperties,
            Properties containerProperties) {

        YarnClientCustomization customization = YarnClientCustomization.getCustomization(clientName);
        if (customization == null) {
            return;
        }
        String dir = jobServiceHelper.createJobDir();
        containerProperties.put(ContainerProperty.JOBDIR.name(), dir);
        containerProperties.put(ContainerProperty.RUNTIME_CONFIG.name(), runtimeConfig);
        customization.beforeCreateLocalLauncherContextFile(containerProperties);
        String fileName = createContainerLauncherContextFile(customization, appMasterProperties, containerProperties);
        containerProperties.put(ContainerProperty.APPMASTER_CONTEXT_FILE.name(), fileName);
        String containerCount = appMasterProperties.getProperty(AppMasterProperty.CONTAINER_COUNT.name(), "1");

        containerProperties.put(AppmasterConstants.CONTAINER_COUNT, containerCount);
        containerProperties.setProperty(AppMasterProperty.QUEUE.name(),
                appMasterProperties.getProperty(AppMasterProperty.QUEUE.name()));
        containerProperties.setProperty(AppMasterProperty.CUSTOMER.name(),
                appMasterProperties.getProperty(AppMasterProperty.CUSTOMER.name()));
        ResourceLocalizer resourceLocalizer = customization.getResourceLocalizer(containerProperties);
        int memory = customization.getMemory(appMasterProperties);
        int virtualCores = customization.getVirtualcores(appMasterProperties);
        int priority = customization.getPriority(appMasterProperties);
        int maxAppAttempts = customization.getMaxAppAttempts(appMasterProperties);
        String queue = customization.getQueue(appMasterProperties);

        customization.afterCreateLocalLauncherContextFile(containerProperties);
        List<String> commands = customization.getCommands(containerProperties, appMasterProperties);
        Map<String, String> environment = customization.setEnvironment(client.getEnvironment(), containerProperties);
        client.setAppName(jobServiceHelper.appName(appMasterProperties, clientName));
        if (resourceLocalizer != null) {
            client.setResourceLocalizer(resourceLocalizer);
        }

        if (memory > 0) {
            client.setMemory(memory);
        }

        if (virtualCores > 0) {
            client.setVirtualcores(virtualCores);
        }

        if (priority > 0) {
            client.setPriority(priority);
        }

        if (queue != null) {
            client.setQueue(queue);
        }

        if (commands != null) {
            client.setCommands(commands);
        }

        if (environment != null) {
            client.setEnvironment(environment);
        }

//        if (maxAppAttempts != 0) {
//            client.setMaxAppAttempts(maxAppAttempts);
//        }

        jobServiceHelper.writeMetadataJson(containerProperties);
    }

    private String createContainerLauncherContextFile(YarnClientCustomization customization,
            Properties appMasterProperties, Properties containerProperties) {
        String contextFileName = customization.getContainerLauncherContextFile(appMasterProperties);

        try (InputStream contextFileUrlFromClasspathAsStream = getClass().getResourceAsStream(contextFileName)) {
            String sb = "";
            try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
                FileCopyUtils.copy(contextFileUrlFromClasspathAsStream, stream);
                sb = new String(stream.toByteArray());
            }
            if (containerProperties != null) {
                for (Map.Entry<Object, Object> entry : containerProperties.entrySet()) {
                    sb = sb.replaceAll("\\$\\$" + entry.getKey().toString() + "\\$\\$", entry.getValue().toString());
                }
            }
            contextFileName = contextFileName.substring(1);
            contextFileName = contextFileName.replaceFirst("/", "-");
            String dir = containerProperties.getProperty(ContainerProperty.JOBDIR.name());
            File contextFile = new File(dir + "/" + contextFileName);
            FileUtils.write(contextFile, sb, Charset.defaultCharset(), false);
            return contextFile.getAbsolutePath();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void validate(CommandYarnClient client, String clientName, Properties appMasterProperties,
            Properties containerProperties) {
        YarnClientCustomization customization = YarnClientCustomization.getCustomization(clientName);
        if (customization == null) {
            return;
        }
        customization.validate(appMasterProperties, containerProperties);
    }

    @Override
    public void finalize(String clientName, Properties appMasterProperties, Properties containerProperties) {
        String dir = containerProperties.getProperty(ContainerProperty.JOBDIR.name());
        try {
            FileUtils.deleteDirectory(new File(dir));
        } catch (IOException e) {
            log.warn("Could not delete local job directory.");
        }
        YarnClientCustomization customization = YarnClientCustomization.getCustomization(clientName);
        customization.finalize(appMasterProperties, containerProperties);
    }

    public void setConfiguration(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }

}

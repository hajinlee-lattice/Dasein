package com.latticeengines.dataplatform.service.impl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;
import org.springframework.yarn.client.CommandYarnClient;
import org.springframework.yarn.fs.ResourceLocalizer;

import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.service.JobNameService;
import com.latticeengines.dataplatform.service.YarnClientCustomizationService;
import com.latticeengines.dataplatform.util.HdfsHelper;
import com.latticeengines.dataplatform.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.yarn.client.ContainerProperty;
import com.latticeengines.dataplatform.yarn.client.YarnClientCustomization;
import com.latticeengines.dataplatform.yarn.client.YarnClientCustomizationRegistry;

@Component("yarnClientCustomizationService")
public class YarnClientCustomizationServiceImpl implements YarnClientCustomizationService {

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private YarnClientCustomizationRegistry yarnClientCustomizationRegistry;

    @Autowired
    private JobNameService jobNameService;

    @Override
    public void addCustomizations(CommandYarnClient client, String clientName, Properties appMasterProperties,
            Properties containerProperties) {

        YarnClientCustomization customization = yarnClientCustomizationRegistry.getCustomization(clientName);
        if (customization == null) {
            return;
        }
        String dir = UUID.randomUUID().toString();
        try {
            HdfsHelper.mkdir(yarnConfiguration, "/app/dataplatform/" + dir);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00000, e, new String[] { dir });
        }
        containerProperties.put(ContainerProperty.JOBDIR.name(), dir);
        customization.beforeCreateLocalLauncherContextFile(containerProperties);
        String fileName = createContainerLauncherContextFile(customization, appMasterProperties, containerProperties);
        containerProperties.put(ContainerProperty.APPMASTER_CONTEXT_FILE.name(), fileName);
        containerProperties.setProperty(AppMasterProperty.QUEUE.name(),
                appMasterProperties.getProperty(AppMasterProperty.QUEUE.name()));
        containerProperties.setProperty(AppMasterProperty.CUSTOMER.name(),
                appMasterProperties.getProperty(AppMasterProperty.CUSTOMER.name()));
        ResourceLocalizer resourceLocalizer = customization.getResourceLocalizer(containerProperties);
        int memory = customization.getMemory(appMasterProperties);
        int virtualCores = customization.getVirtualcores(appMasterProperties);
        int priority = customization.getPriority(appMasterProperties);
        String queue = customization.getQueue(appMasterProperties);
        List<String> commands = customization.getCommands(containerProperties);

        client.setAppName(jobNameService.createJobName(
                appMasterProperties.getProperty(AppMasterProperty.CUSTOMER.name()), clientName));
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

    }

    private String createContainerLauncherContextFile(YarnClientCustomization customization,
            Properties appMasterProperties, Properties containerProperties) {
        String contextFileName = customization.getContainerLauncherContextFile(appMasterProperties);
        InputStream contextFileUrlFromClasspathAsStream = getClass().getResourceAsStream(contextFileName);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            FileCopyUtils.copy(contextFileUrlFromClasspathAsStream, stream);
            String sb = new String(stream.toByteArray());

            if (containerProperties != null) {
                for (Map.Entry<Object, Object> entry : containerProperties.entrySet()) {
                    sb = sb.replaceAll("\\$\\$" + entry.getKey().toString() + "\\$\\$", entry.getValue().toString());
                }
            }
            contextFileName = contextFileName.substring(1);
            contextFileName = contextFileName.replaceFirst("/", "-");

            File contextFile = new File(contextFileName);
            FileUtils.write(contextFile, sb);
            return contextFile.getAbsolutePath();

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void validate(CommandYarnClient client, String clientName, Properties appMasterProperties,
            Properties containerProperties) {
        YarnClientCustomization customization = yarnClientCustomizationRegistry.getCustomization(clientName);
        if (customization == null) {
            return;
        }
        customization.validate(appMasterProperties, containerProperties);
    }

    @Override
    public void finalize(String clientName, Properties appMasterProperties, Properties containerProperties) {
        YarnClientCustomization customization = yarnClientCustomizationRegistry.getCustomization(clientName);
        customization.finalize(appMasterProperties, containerProperties);
    }

}

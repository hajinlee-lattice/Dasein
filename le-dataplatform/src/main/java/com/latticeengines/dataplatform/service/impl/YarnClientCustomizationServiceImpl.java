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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;
import org.springframework.yarn.am.AppmasterConstants;
import org.springframework.yarn.client.CommandYarnClient;
import org.springframework.yarn.fs.ResourceLocalizer;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.service.JobNameService;
import com.latticeengines.dataplatform.exposed.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.YarnClientCustomization;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.dataplatform.service.YarnClientCustomizationService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.Classifier;

@Component("yarnClientCustomizationService")
public class YarnClientCustomizationServiceImpl implements YarnClientCustomizationService {

    private static final Log log = LogFactory.getLog(YarnClientCustomizationServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private JobNameService jobNameService;

    @Value("${dataplatform.yarn.job.basedir}")
    private String hdfsJobBaseDir;

    @Value("${dataplatform.yarn.job.runtime.config}")
    private String runtimeConfig;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Override
    public void addCustomizations(CommandYarnClient client, String clientName, Properties appMasterProperties,
            Properties containerProperties) {

        YarnClientCustomization customization = YarnClientCustomization.getCustomization(clientName);
        if (customization == null) {
            return;
        }
        String dir = UUID.randomUUID().toString();
        try {
            HdfsUtils.mkdir(yarnConfiguration, hdfsJobBaseDir + "/" + dir);
            new File("./" + dir).mkdir();
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00000, e, new String[] { dir });
        }
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
        
        int memory = customization.getMemory(containerProperties);
        int virtualCores = customization.getVirtualcores(containerProperties);
        int priority = customization.getPriority(containerProperties);
        String queue = customization.getQueue(appMasterProperties);

        customization.afterCreateLocalLauncherContextFile(containerProperties);
        List<String> commands = customization.getCommands(containerProperties);
        Map<String, String> environment = customization.setEnvironment(client.getEnvironment(), containerProperties);
        client.setAppName(appName(appMasterProperties, clientName));
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

        // copy the metadata.json file to HDFS data directory
        String jobType = containerProperties.getProperty(ContainerProperty.JOB_TYPE.name());
        if (jobType != null) {
            String metadata = containerProperties.getProperty(PythonContainerProperty.METADATA_CONTENTS.name());
            Classifier classifier = JsonUtils.deserialize(metadata, Classifier.class);
            String hdfsDir = classifier.getDataDiagnosticsPath();
            hdfsDir = hdfsDir.substring(0, hdfsDir.lastIndexOf('/') + 1);
            String localDir = containerProperties.getProperty(PythonContainerProperty.METADATA.name());
            String random = "-" + UUID.randomUUID().toString();
            String metaDataFileName = "metadata-" + jobType + random + ".json";
            try {
                HdfsUtils.copyLocalToHdfs(yarnConfiguration, localDir, hdfsDir + metaDataFileName);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_00000, e, new String[] { hdfsDir });
            }
        }
    }

    protected String appName(Properties appMasterProperties, String clientName) {
        if (appMasterProperties.containsKey(AppMasterProperty.APP_NAME.name())) {
            return appMasterProperties.getProperty(AppMasterProperty.APP_NAME.name());
        } else if (appMasterProperties.containsKey(AppMasterProperty.APP_NAME_SUFFIX.name())) {
            return jobNameService.createJobName(appMasterProperties.getProperty(AppMasterProperty.CUSTOMER.name()),
                    clientName)
                    + JobNameServiceImpl.JOBNAME_DELIMITER
                    + appMasterProperties.getProperty(AppMasterProperty.APP_NAME_SUFFIX.name());
        } else {
            return jobNameService.createJobName(appMasterProperties.getProperty(AppMasterProperty.CUSTOMER.name()),
                    clientName);
        }
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
            FileUtils.write(contextFile, sb);
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
            log.warn("Could not delete local job directory.", e);
        }
        YarnClientCustomization customization = YarnClientCustomization.getCustomization(clientName);
        customization.finalize(appMasterProperties, containerProperties);
    }

}

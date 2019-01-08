package com.latticeengines.yarn.exposed.service.impl;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.yarn.client.CommandYarnClient;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.yarn.RuntimeConfig;
import com.latticeengines.yarn.exposed.client.AppMasterProperty;
import com.latticeengines.yarn.exposed.client.ContainerProperty;
import com.latticeengines.yarn.exposed.client.YarnClientCustomization;
import com.latticeengines.yarn.exposed.service.YarnClientCustomizationService;

@Component("awsBatchCustomizationService")
public class AwsBatchCustomizationServiceImpl implements YarnClientCustomizationService {

    private static final Logger log = LoggerFactory.getLogger(AwsBatchCustomizationServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${dataplatform.yarn.job.runtime.config}")
    private String runtimeConfig;

    @Autowired
    private YarnClientCustomizationService yarnClientCustomizationService;

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
        containerProperties.setProperty(AppMasterProperty.CUSTOMER.name(),
                appMasterProperties.getProperty(AppMasterProperty.CUSTOMER.name()));
        setRuntimeConfig(containerProperties);
        writeMetadataToJobDir(containerProperties);
        jobServiceHelper.writeMetadataJson(containerProperties);
    }

    private void writeMetadataToJobDir(Properties containerProperties) {
        try {
            String metadataFilePath = containerProperties.getProperty(ContainerProperty.METADATA.name());
            HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, metadataFilePath,
                    jobServiceHelper.getJobDir(containerProperties));
        } catch (Exception ex) {
            throw new RuntimeException("Failed to write metadata json to hdfs.", ex);
        }
    }

    @Override
    public void validate(CommandYarnClient client, String clientName, Properties appMasterProperties,
            Properties containerProperties) {
        yarnClientCustomizationService.validate(client, clientName, appMasterProperties, containerProperties);
    }

    @Override
    public void finalize(String clientName, Properties appMasterProperties, Properties containerProperties) {
        yarnClientCustomizationService.finalize(clientName, appMasterProperties, containerProperties);
    }

    private void setRuntimeConfig(Properties parameters) {
        String configPath = jobServiceHelper.getJobDir(parameters) + "/"
                + parameters.getProperty(ContainerProperty.RUNTIME_CONFIG.name());
        RuntimeConfig runtimeConfig = new RuntimeConfig(configPath, yarnConfiguration);
        runtimeConfig.addProperties("host", "127.0.0.1");
        runtimeConfig.addProperties("port", "0");
        runtimeConfig.writeToHdfs();
    }

}

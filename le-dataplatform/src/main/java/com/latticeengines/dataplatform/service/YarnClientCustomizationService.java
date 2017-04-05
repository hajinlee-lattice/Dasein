package com.latticeengines.dataplatform.service;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.springframework.yarn.client.CommandYarnClient;

public interface YarnClientCustomizationService {

    void addCustomizations(CommandYarnClient client, String clientName, Properties appMasterProperties,
            Properties containerProperties);

    void validate(CommandYarnClient client, String clientName, Properties appMasterProperties,
            Properties containerProperties);

    void finalize(String clientName, Properties appMasterProperties, Properties containerProperties);

    void setConfiguration(Configuration yarnConfiguration);
}

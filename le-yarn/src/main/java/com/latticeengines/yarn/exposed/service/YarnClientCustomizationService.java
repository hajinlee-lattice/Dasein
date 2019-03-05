package com.latticeengines.yarn.exposed.service;

import java.util.Properties;

import org.springframework.yarn.client.CommandYarnClient;

public interface YarnClientCustomizationService {

    void addCustomizations(CommandYarnClient client, String clientName, Properties appMasterProperties,
            Properties containerProperties);

    void validate(CommandYarnClient client, String clientName, Properties appMasterProperties,
            Properties containerProperties);

    void finalize(String clientName, Properties appMasterProperties, Properties containerProperties);

}

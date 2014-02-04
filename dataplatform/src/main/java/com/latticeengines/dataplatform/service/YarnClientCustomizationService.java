package com.latticeengines.dataplatform.service;

import java.util.Properties;

import org.springframework.yarn.client.CommandYarnClient;

public interface YarnClientCustomizationService {

	void addCustomizations(CommandYarnClient client, String clientName, Properties appMasterProperties, Properties containerProperties);
}

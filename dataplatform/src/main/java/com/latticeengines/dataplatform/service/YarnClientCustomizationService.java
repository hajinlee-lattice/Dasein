package com.latticeengines.dataplatform.service;

import java.util.Map;

import org.springframework.yarn.client.CommandYarnClient;

public interface YarnClientCustomizationService {

	void addCustomizations(CommandYarnClient client, String clientName, Map<String, String> containerProperties);
}

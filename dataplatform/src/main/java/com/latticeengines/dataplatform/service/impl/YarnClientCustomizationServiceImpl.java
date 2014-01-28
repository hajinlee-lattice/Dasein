package com.latticeengines.dataplatform.service.impl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;
import org.springframework.yarn.client.CommandYarnClient;
import org.springframework.yarn.fs.ResourceLocalizer;

import com.latticeengines.dataplatform.service.YarnClientCustomizationService;
import com.latticeengines.dataplatform.yarn.client.ContainerProperty;
import com.latticeengines.dataplatform.yarn.client.YarnClientCustomization;
import com.latticeengines.dataplatform.yarn.client.YarnClientCustomizationRegistry;

@Component("yarnClientCustomizationService")
public class YarnClientCustomizationServiceImpl implements
		YarnClientCustomizationService {

	@Autowired
	private YarnClientCustomizationRegistry yarnClientCustomizationRegistry;

	@Override
	public void addCustomizations(CommandYarnClient client, String clientName,
			Properties containerProperties) {
		
		YarnClientCustomization customization = yarnClientCustomizationRegistry.getCustomization(clientName);
		if (customization == null) {
			return;
		}
		String fileName = createContainerLauncherContextFile(customization, containerProperties);
		containerProperties.put(ContainerProperty.APPMASTER_CONTEXT_FILE.name(), fileName);
		ResourceLocalizer resourceLocalizer = customization.getResourceLocalizer(containerProperties);
		int memory = customization.getMemory();
		int virtualCores = customization.getVirtualcores();
		int priority = customization.getPriority();
		String queue = customization.getQueue();
		List<String> commands = customization.getCommands(containerProperties);

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

	private String createContainerLauncherContextFile(
			YarnClientCustomization customization,
			Properties containerProperties) {
		String contextFileName = customization.getContainerLauncherContextFile();
		InputStream contextFileUrlFromClasspathAsStream = ClassLoader
				.getSystemResourceAsStream(contextFileName);
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		try {
			FileCopyUtils.copy(contextFileUrlFromClasspathAsStream, stream);
			String sb = new String(stream.toByteArray());
			
			if (containerProperties != null) {			
				for (Map.Entry<Object, Object> entry : containerProperties.entrySet()) {
					sb = sb.replaceAll("\\$\\$" + entry.getKey().toString() + "\\$\\$", entry.getValue().toString());
				}
			}
			contextFileName = contextFileName.replaceFirst("/", "-");
			File contextFile = new File(contextFileName + "-" + System.currentTimeMillis());
			FileUtils.write(contextFile, sb);
			return contextFile.getAbsolutePath();

		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

}

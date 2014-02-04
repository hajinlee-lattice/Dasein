package com.latticeengines.dataplatform.yarn.client;

import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;
import org.springframework.yarn.fs.ResourceLocalizer;

public interface YarnClientCustomization {
	
	String getClientId();
	
	void beforeCreateLocalLauncherContextFile(Properties properties);

	ResourceLocalizer getResourceLocalizer(Properties properties);
	
	Collection<CopyEntry> getCopyEntries(Properties properties);
	
	Collection<TransferEntry> getHdfsEntries(Properties properties);
	
	int getMemory(Properties properties);
	
	int getPriority(Properties properties);
	
	String getQueue(Properties properties);
	
	int getVirtualcores(Properties properties);
	
	String getContainerLauncherContextFile(Properties properties);
	
	List<String> getCommands(Properties properties);
}

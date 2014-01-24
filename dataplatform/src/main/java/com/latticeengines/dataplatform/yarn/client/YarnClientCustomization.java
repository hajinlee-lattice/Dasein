package com.latticeengines.dataplatform.yarn.client;

import java.util.Collection;
import java.util.List;

import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;
import org.springframework.yarn.fs.ResourceLocalizer;

public interface YarnClientCustomization {
	
	String getClientId();

	ResourceLocalizer getResourceLocalizer();
	
	Collection<CopyEntry> getCopyEntries();
	
	Collection<TransferEntry> getHdfsEntries();
	
	int getMemory();
	
	int getPriority();
	
	String getQueue();
	
	int getVirtualcores();
	
	String getContainerLauncherContextFile();
	
	List<String> getCommands();
}

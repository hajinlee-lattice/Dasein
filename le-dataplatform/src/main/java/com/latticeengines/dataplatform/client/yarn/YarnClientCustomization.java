package com.latticeengines.dataplatform.client.yarn;

import java.util.Collection;
import java.util.List;
import java.util.Map;
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

    void validate(Properties appMasterProperties, Properties containerProperties);

    void finalize(Properties appMasterProperties, Properties containerProperties);

    Map<String, String> setEnvironment(Map<String, String> environment, Properties containerProperties);
}

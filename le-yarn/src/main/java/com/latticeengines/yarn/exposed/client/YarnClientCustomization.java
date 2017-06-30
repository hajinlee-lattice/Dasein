package com.latticeengines.yarn.exposed.client;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;
import org.springframework.yarn.fs.ResourceLocalizer;

public abstract class YarnClientCustomization {

    protected Configuration yarnConfiguration;

    private static Map<String, YarnClientCustomization> registry = new HashMap<>();

    public YarnClientCustomization() {
        registry.put(getClientId(), this);
    }

    public static YarnClientCustomization getCustomization(String clientId) {
        return registry.get(clientId);
    }

    public abstract String getClientId();

    public abstract void beforeCreateLocalLauncherContextFile(Properties properties);

    public abstract ResourceLocalizer getResourceLocalizer(Properties properties);

    public abstract Collection<CopyEntry> getCopyEntries(Properties properties);

    public abstract Collection<TransferEntry> getHdfsEntries(Properties properties);

    public abstract int getMemory(Properties properties);

    public abstract int getPriority(Properties properties);

    public abstract String getQueue(Properties properties);

    public abstract int getVirtualcores(Properties properties);

    public abstract String getContainerLauncherContextFile(Properties properties);

    public abstract List<String> getCommands(Properties properties);

    public abstract void validate(Properties appMasterProperties, Properties containerProperties);

    public abstract void finalize(Properties appMasterProperties, Properties containerProperties);

    public abstract Map<String, String> setEnvironment(Map<String, String> environment, Properties containerProperties);

    public abstract void afterCreateLocalLauncherContextFile(Properties containerProperties);

    public abstract int getMaxAppAttempts(Properties appMasterProperties);

}

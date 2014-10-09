package com.latticeengines.dataplatform.client.yarn;

import java.util.Collection;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;

import com.latticeengines.dataplatform.runtime.jetty.JettyContainerProperty;

public class JettyClientCustomization extends DefaultYarnClientCustomization {

    public JettyClientCustomization(Configuration configuration, String hdfsJobBaseDir) {
        super(configuration, hdfsJobBaseDir);
    }

    @Override
    public String getClientId() {
        return "jettyClient";
    }

    @Override
    public String getContainerLauncherContextFile(Properties properties) {
        return "/jetty/dataplatform-jetty-appmaster-context.xml";
    }

    @Override
    public void beforeCreateLocalLauncherContextFile(Properties properties) {
        try {
            properties.put(JettyContainerProperty.WAR_FILE.name(), properties.getProperty(ContainerProperty.METADATA.name()) + ".war");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Collection<CopyEntry> getCopyEntries(Properties containerProperties) {
        Collection<CopyEntry> copyEntries = super.getCopyEntries(containerProperties);

        return copyEntries;

    }

    @Override
    public void validate(Properties appMasterProperties, Properties containerProperties) {
    }

}

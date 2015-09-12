package com.latticeengines.dataplatform.exposed.yarn.client;

import java.io.File;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;

public abstract class SingleContainerClientCustomization extends DefaultYarnClientCustomization {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(SingleContainerClientCustomization.class);
    
    @Autowired
    public SingleContainerClientCustomization(Configuration yarnConfiguration, //
            String hdfsJobBaseDir, //
            String webHdfs) {
        super(yarnConfiguration, hdfsJobBaseDir, webHdfs);
    }

    public abstract String getModuleName();
    
    @Override
    public String getClientId() {
        return getModuleName() + "Client";
    }

    @Override
    public String getContainerLauncherContextFile(Properties properties) {
        return String.format("/%s-appmaster-context.xml", getModuleName());
    }

    @Override
    public void beforeCreateLocalLauncherContextFile(Properties properties) {
        try {
            String dir = properties.getProperty(ContainerProperty.JOBDIR.name());
            String importConfig = (String) properties.remove(getModuleName() + "Config");
            File metadataFile = new File(dir + "/metadata.json");
            FileUtils.writeStringToFile(metadataFile, importConfig);
            properties.put(ContainerProperty.METADATA.name(), metadataFile.getAbsolutePath());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Collection<CopyEntry> getCopyEntries(Properties containerProperties) {
        Collection<CopyEntry> copyEntries = super.getCopyEntries(containerProperties);
        String metadataFilePath = containerProperties.getProperty(ContainerProperty.METADATA.name());
        copyEntries.add(new LocalResourcesFactoryBean.CopyEntry("file:" + metadataFilePath,
                getJobDir(containerProperties), false));
        return copyEntries;
    }

    @Override
    public Collection<TransferEntry> getHdfsEntries(Properties containerProperties) {
        Collection<LocalResourcesFactoryBean.TransferEntry> hdfsEntries = super.getHdfsEntries(containerProperties);
        String module = getModuleName();
        hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                LocalResourceVisibility.PUBLIC, //
                String.format("/app/%s/%s.properties", module, module), //
                false));
        hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                LocalResourceVisibility.PUBLIC, //
                String.format("/app/%s/lib/*", module), //
                false));
        return hdfsEntries;
    }

}

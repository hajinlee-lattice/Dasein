package com.latticeengines.dataplatform.exposed.yarn.client;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.springframework.yarn.fs.LocalResourcesFactoryBean;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry;
import org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;
import com.latticeengines.swlib.exposed.service.SoftwareLibraryService;

public abstract class SingleContainerClientCustomization extends DefaultYarnClientCustomization {

    private static final Log log = LogFactory.getLog(SingleContainerClientCustomization.class);

    public SingleContainerClientCustomization(Configuration yarnConfiguration, //
            VersionManager versionManager, //
            String stackName, //
            String hdfsJobBaseDir, //
            String webHdfs) {
        this(yarnConfiguration, versionManager, stackName, null, hdfsJobBaseDir, webHdfs);
    }

    public SingleContainerClientCustomization(Configuration yarnConfiguration, //
            VersionManager versionManager, //
            String stackName, //
            SoftwareLibraryService softwareLibraryService, //
            String hdfsJobBaseDir, //
            String webHdfs) {
        super(yarnConfiguration, versionManager, stackName, softwareLibraryService, hdfsJobBaseDir, webHdfs);
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
            String importConfig = properties.getProperty(getModuleName() + "Config");
            File metadataFile = new File(dir + "/metadata.json");
            FileUtils.writeStringToFile(metadataFile, importConfig);
            properties.put(ContainerProperty.METADATA.name(), metadataFile.getAbsolutePath());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void afterCreateLocalLauncherContextFile(Properties containerProperties) {
        containerProperties.remove(getModuleName() + "Config");
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
        Collection<LocalResourcesFactoryBean.TransferEntry> hdfsEntries = getHdfsEntries(containerProperties, true);
        hdfsEntries = appendModuleHdfsEntries(hdfsEntries);
        return hdfsEntries;
    }

    protected Collection<LocalResourcesFactoryBean.TransferEntry> appendModuleHdfsEntries(
            Collection<LocalResourcesFactoryBean.TransferEntry> hdfsEntries) {
        String module = getModuleName();
        hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                LocalResourceVisibility.PUBLIC, //
                String.format("/app/%s/%s/lib/*", versionManager.getCurrentVersionInStack(stackName), module), //
                false));
        if (softwareLibraryService != null) {
            softwareLibraryService.setStackName(stackName);
            List<SoftwarePackage> packages = softwareLibraryService.getInstalledPackagesByVersion(module,
                    versionManager.getCurrentVersion());
            if (StringUtils.isEmpty(versionManager.getCurrentVersion())) {
                packages = softwareLibraryService.getLatestInstalledPackages(module);
            }

            for (SoftwarePackage pkg : packages) {
                String hdfsJar = String.format("%s/%s", //
                        softwareLibraryService.getTopLevelPath(), pkg.getHdfsPath());
                log.info(String.format("Adding %s to hdfs entry.", hdfsJar));
                hdfsEntries.add(new LocalResourcesFactoryBean.TransferEntry(LocalResourceType.FILE, //
                        LocalResourceVisibility.PUBLIC, //
                        hdfsJar, //
                        false));
            }
        }
        return hdfsEntries;
    }

}

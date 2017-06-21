package com.latticeengines.swlib.exposed.service;

import java.io.File;
import java.util.List;

import org.springframework.context.ApplicationContext;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;

public interface SoftwareLibraryService {

    void setStackName(String stackName);

    void installPackage(SoftwarePackage swPackage, File localFile);

    @Deprecated
    List<SoftwarePackage> getInstalledPackages(String module);

    List<SoftwarePackage> getLatestInstalledPackages(String module);

    List<SoftwarePackage> getInstalledPackagesByVersion(String module, String version);

    void installPackage(String fsDefaultFS, SoftwarePackage swPackage, File localFile);

    String getTopLevelPath();

    ApplicationContext loadSoftwarePackages(String module, ApplicationContext context, VersionManager versionManager);

    ApplicationContext loadSoftwarePackages(String module, String name, ApplicationContext context,
                         VersionManager versionManager);
}

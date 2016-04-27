package com.latticeengines.swlib.exposed.service;

import java.io.File;
import java.util.List;

import com.latticeengines.domain.exposed.swlib.SoftwarePackage;

public interface SoftwareLibraryService {

    void setStackName(String stackName);

    void installPackage(SoftwarePackage swPackage, File localFile);

    List<SoftwarePackage> getInstalledPackages(String module);

    List<SoftwarePackage> getLatestInstalledPackages(String module);

    List<SoftwarePackage> getInstalledPackagesByVersion(String module, String version);

    void installPackage(String fsDefaultFS, SoftwarePackage swPackage, File localFile);

    String getTopLevelPath();

}

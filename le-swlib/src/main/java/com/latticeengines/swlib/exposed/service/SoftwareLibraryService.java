package com.latticeengines.swlib.exposed.service;

import java.io.File;
import java.util.Collection;
import java.util.List;

import org.springframework.context.ApplicationContext;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.swlib.SoftwarePackage;

public interface SoftwareLibraryService {

    void setStackAndVersion(String stackName, String version);

    void installPackage(SoftwarePackage swPackage, File localFile);

    List<SoftwarePackage> getInstalledPackages(String module);

    void installPackage(String fsDefaultFS, SoftwarePackage swPackage, File localFile);

    String getTopLevelPath();

    ApplicationContext loadSoftwarePackages(String module, ApplicationContext context);

    ApplicationContext loadSoftwarePackages(String module, Collection<String> names, ApplicationContext context);
}

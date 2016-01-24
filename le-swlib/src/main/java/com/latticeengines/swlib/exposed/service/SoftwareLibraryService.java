package com.latticeengines.swlib.exposed.service;

import java.io.File;
import java.util.List;

import com.latticeengines.domain.exposed.swlib.SoftwarePackage;

public interface SoftwareLibraryService {
    
    public static final String TOPLEVELPATH = "/app/swlib";

    void installPackage(SoftwarePackage swPackage, File localFile);
    
    List<SoftwarePackage> getInstalledPackages(String module);

    List<SoftwarePackage> getLatestInstalledPackages(String module);

    void installPackage(String fsDefaultFS, SoftwarePackage swPackage, File localFile);

}

package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.AtlasExportType;

public interface AtlasExportService {

    AtlasExport createAtlasExport(String customerSpace, AtlasExportType exportType);

//    void updateAtlasExportSystemPath(String customerSpace, String uuid, String systemPath);
//
//    void updateAtlasExportDropfolderpath(String customerSpace, String uuid, String dropfolderPath);

    void addFileToSystemPath(String customerSpace, String uuid, String fileName);

    void addFileToDropFolder(String customerSpace, String uuid, String fileName);

    AtlasExport getAtlasExport(String customerSpace, String uuid);

}

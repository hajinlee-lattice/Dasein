package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.AtlasExportType;

public interface AtlasExportService {

    AtlasExport createAtlasExport(String customerSpace, AtlasExportType exportType);

    void addFileToSystemPath(String customerSpace, String uuid, String fileName);

    void addFileToDropFolder(String customerSpace, String uuid, String fileName);

    AtlasExport getAtlasExport(String customerSpace, String uuid);

}

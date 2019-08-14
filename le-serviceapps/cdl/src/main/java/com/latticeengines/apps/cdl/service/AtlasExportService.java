package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.AtlasExportType;

public interface AtlasExportService {

    AtlasExport createAtlasExport(String customerSpace, AtlasExportType exportType);

    void deleteAtlasExport(String customerSpace, String uuid);

    void addFileToSystemPath(String customerSpace, String uuid, String fileName, List<String> filesToDelete);

    void addFileToDropFolder(String customerSpace, String uuid, String fileName, List<String> filesToDelete);

    AtlasExport getAtlasExport(String customerSpace, String uuid);

    List<AtlasExport> findAll(String customerSpace);

    void updateAtlasExport(String customerSpace, AtlasExport atlasExport);

    AtlasExport createAtlasExport(String customerSpace, AtlasExport atlasExport);

}

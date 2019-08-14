package com.latticeengines.proxy.exposed.cdl;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;

public interface AtlasExportProxy {

    List<AtlasExport> findAll(String customerSpace);

    AtlasExport createAtlasExport(String customerSpace, AtlasExport atlasExport);

    AtlasExport findAtlasExportById(String customerSpace, String uuid);

    void updateAtlasExportStatus(String customerSpace, String uuid, MetadataSegmentExport.Status status);

    void addFileToSystemPath(String customerSpace, String uuid, String fileName);

    void addFileToDropFolder(String customerSpace, String uuid, String fileName);

    String getDropFolderExportPath(String customerSpace, AtlasExportType exportType, String datePrefix,
                                   boolean withProtocol);

    String getSystemExportPath(String customerSpace, boolean withProtocol);

    String getS3PathWithProtocol(String customerSpace, String relativePath);
}

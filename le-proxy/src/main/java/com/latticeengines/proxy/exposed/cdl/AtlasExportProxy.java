package com.latticeengines.proxy.exposed.cdl;

import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.AtlasExportType;

public interface AtlasExportProxy {

    AtlasExport findAtlasExportById(String customerSpace, String uuid);

    void addFileToSystemPath(String customerSpace, String uuid, String fileName);

    void addFileToDropFolder(String customerSpace, String uuid, String fileName);

    String getDropFolderExportPath(String customerSpace, AtlasExportType exportType, String datePrefix,
                                   boolean withProtocol);

    String getSystemExportPath(String customerSpace, boolean withProtocol);

    String getS3PathWithProtocol(String customerSpace, String relativePath);
}

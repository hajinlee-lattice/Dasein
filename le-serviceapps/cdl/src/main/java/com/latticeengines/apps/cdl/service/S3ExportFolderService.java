package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.pls.AtlasExportType;

public interface S3ExportFolderService {

    /**
     *
     * @param customerSpace {@link com.latticeengines.domain.exposed.camille.CustomerSpace}
     * @param exportType {@link AtlasExportType}
     * @param optionalId optional part: play id / segment name etc.
     * @return
     *       /dropfolder/<randomId>/Export/<exportType>/<optionalId>/<datePrefix>/
     */
    String getDropFolderExportPath(String customerSpace, AtlasExportType exportType, String datePrefix,
                                   String optionalId);

    /**
     *
     * @param customerSpace {@link com.latticeengines.domain.exposed.camille.CustomerSpace}
     * @return
     *       /<tenant>/atlas/Data/Files/Export/
     */
    String getSystemExportPath(String customerSpace);

    /**
     *
     * @param customerSpace {@link com.latticeengines.domain.exposed.camille.CustomerSpace}
     * @param relativePath path under bucket
     * @return
     *      s3n://<bucket>/<relativePath>
     */
    String getS3PathWithProtocol(String customerSpace, String relativePath);
}

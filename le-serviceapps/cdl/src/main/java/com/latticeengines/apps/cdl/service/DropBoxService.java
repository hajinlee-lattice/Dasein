package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.security.Tenant;

public interface DropBoxService {

    DropBox create();

    void delete();

    String getDropBoxBucket();

    String getDropBoxPrefix();

    GrantDropBoxAccessResponse grantAccess(GrantDropBoxAccessRequest request);

    GrantDropBoxAccessResponse refreshAccessKey();

    GrantDropBoxAccessResponse getAccessKey();

    void revokeAccess();

    DropBoxSummary getDropBoxSummary();

    Tenant getDropBoxOwner(String dropBox);

    void createTenantDefaultFolder(String customerSpace);

    void createFolder(String customerSpace, String systemName, String objectName, String path);

    List<String> getDropFolders(String customerSpace, String systemName, String objectName, String path);

    boolean uploadFileToS3(String customerSpace, String key, String s3FileName, String hdfsPath);

    List<FileProperty> getFileListForPath(String customerSpace, String s3Path, String filter);

    String getExportPath(String customerSpace, AtlasExportType exportType, String datePrefix, String optionalId);
}

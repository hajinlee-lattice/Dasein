package com.latticeengines.apps.core.service;

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

    /**
     *
     * @param customerSpace: TenantId
     * @param relativePath: Should be relative path under dropbox, not absolute path in bucket.
     * @param filter: File extension filter.
     * @return List of {@link FileProperty}.
     */
    List<FileProperty> getFileListForPath(String customerSpace, String relativePath, String filter);

    String getExportPath(String customerSpace, AtlasExportType exportType, String datePrefix, String optionalId);

    void createSubFolder(String customerSpace, String systemName, String objectName, String path);

    List<String> getDropFoldersFromSystem(String customerSpace, String systemName);

    void createFolderUnderDropFolder(String path);

    /**
     * Remove the template path based on feedType.
     * A template path should be: dropfolder/{dropfolderid}/Templates/{feedType}
     * @param customerSpace: TenantId
     * @param feedType: eg: AccountData, DefaultSystem_AccountData.
     */
    void removeTemplatePath(String customerSpace, String feedType);

    /**
     * Re-create template path based on feedType
     * Re-created template path will be: dropfolder/{dropfolderid}/Templates/{feedType}
     * @param customerSpace: TenantId
     * @param feedType: (Same as remove)
     */
    void restoreTemplatePath(String customerSpace, String feedType);
}

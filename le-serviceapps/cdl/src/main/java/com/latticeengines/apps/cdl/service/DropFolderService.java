package com.latticeengines.apps.cdl.service;

import java.util.List;

public interface DropFolderService {
    void createTenantDefaultFolder(String customerSpace);
    void createFolder(String customerSpace, String objectName, String path);

    List<String> getDropFolders(String customerSpace, String objectName, String path);
}

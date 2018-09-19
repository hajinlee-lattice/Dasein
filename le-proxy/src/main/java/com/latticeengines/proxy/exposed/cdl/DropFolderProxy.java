package com.latticeengines.proxy.exposed.cdl;

import java.util.List;

public interface DropFolderProxy {
    boolean createTemplateFolder(String customerSpace, String objectName, String path);
    List<String> getAllSubFolders(String customerSpace, String objectName,  String path);
}

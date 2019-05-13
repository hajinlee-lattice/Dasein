package com.latticeengines.proxy.exposed.cdl;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.pls.FileProperty;

public interface DropBoxProxy {

    DropBoxSummary getDropBox(String customerSpace);

    GrantDropBoxAccessResponse grantAccess(String customerSpace, GrantDropBoxAccessRequest request);

    GrantDropBoxAccessResponse refreshAccessKey(String customerSpace);

    boolean createTemplateFolder(String customerSpace, String systemName, String objectName, String path);

    List<String> getAllSubFolders(String customerSpace, String systemName, String objectName, String path);

    boolean importS3file(String customerSpace, String s3Path, String hdfsPath, String filename);

    List<FileProperty> getFileListForPath(String customerSpace, String s3Path, String filter);
}

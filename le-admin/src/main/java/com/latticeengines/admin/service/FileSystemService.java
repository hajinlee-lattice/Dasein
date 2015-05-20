package com.latticeengines.admin.service;

import java.io.File;
import java.util.List;

public interface FileSystemService {

    List<String> filesInDirectory(File dir);

    void deleteFile(File file);

}

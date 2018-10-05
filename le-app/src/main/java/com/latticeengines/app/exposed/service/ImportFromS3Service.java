package com.latticeengines.app.exposed.service;

import java.util.List;

import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;

public interface ImportFromS3Service {

    String exploreS3FilePath(String inputFile, String customer);

    List<String> getFilesForDir(String prefix, HdfsFilenameFilter filter);

    String getS3Bucket();

}

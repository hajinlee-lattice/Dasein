package com.latticeengines.app.exposed.service;

import java.io.InputStream;
import java.util.List;

import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;

public interface ImportFromS3Service {

    String exploreS3FilePath(String inputFile);

    List<String> getFilesForDir(String prefix, HdfsFilenameFilter filter);

    InputStream getS3FileInputStream(String key);

    InputStream getS3FileInputStream(String bucketName, String key);

    String getS3Bucket();

    String getPodId();

    String getS3FsProtocol();

}

package com.latticeengines.app.exposed.service;

import java.io.InputStream;
import java.util.List;

import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.domain.exposed.metadata.Table;

public interface ImportFromS3Service {

    String exploreS3FilePath(String inputFile);

    List<String> getFilesForDir(String prefix, HdfsFilenameFilter filter);

    void importTable(String customer, Table table, String queueName);

    void importFile(String tenantId, String s3Path, String hdfsPath, String queueNameS);

    InputStream getS3FileInputStream(String key);

    String getS3Bucket();

    String getPodId();

    String getS3FsProtocol();

}

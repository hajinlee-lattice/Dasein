package com.latticeengines.datacloud.core.entitymgr.impl;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.S3SourceEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;

@Service("s3SourceEntityMgr")
public class S3SourceEntityMgrImpl implements S3SourceEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(S3SourceEntityMgrImpl.class);

    @Inject
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private S3Service s3Service;

    @Inject
    private Configuration yarnConfiguration;

    private String s3Bucket = "latticeengines-dev-datacloud";

    @Override
    public void downloadToHdfs(String sourceName, String version, String distCpQueue) {
        System.out.println("Exists in S3: " + sourceSchemaExistsInS3(sourceName, version));
        if (!sourceExistsInS3(sourceName, version)) {
            throw new RuntimeException(String.format("Source %s at %s does not exists in S3", sourceName, version));
        }
        hdfsSourceEntityMgr.deleteSource(sourceName, version);
        String hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, version).toString();
        String s3Uri = hdfsPathBuilder.constructSnapshotDir(sourceName, version).toS3NUri(s3Bucket);
        try {
            HdfsUtils.distcp(yarnConfiguration, s3Uri, hdfsPath, distCpQueue);
        } catch (Exception e) {
            throw new RuntimeException("Failed to distcp " + sourceName + " snapshot at version " + version, e);
        }

        hdfsPath = hdfsPathBuilder.constructSchemaFile(sourceName, version).toString();
        s3Uri = hdfsPathBuilder.constructSchemaFile(sourceName, version).toS3NUri(s3Bucket);
        try {
            HdfsUtils.distcp(yarnConfiguration, s3Uri, hdfsPath, distCpQueue);
        } catch (Exception e) {
            throw new RuntimeException("Failed to distcp " + sourceName + " schema at version " + version, e);
        }

        log.info("Downloaded " + sourceName + " at version " + version + " from S3 to HDFS");
    }

    private boolean sourceExistsInS3(String sourceName, String version) {
        String path = hdfsPathBuilder.constructSnapshotDir(sourceName, version).toS3Key();
        return s3Service.isNonEmptyDirectory(s3Bucket, path);
    }

    private boolean sourceSchemaExistsInS3(String sourceName, String version) {
        String path = hdfsPathBuilder.constructSchemaFile(sourceName, version).toS3Key();
        return s3Service.objectExist(s3Bucket, path);
    }

}

package com.latticeengines.app.exposed.service.impl;

import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;

@Component
public class ImportFromS3ServiceImpl implements ImportFromS3Service {

    private static final Logger log = LoggerFactory.getLogger(ImportFromS3ServiceImpl.class);

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${camille.zk.pod.id:Default}")
    private String podId;

    @Resource(name = "distCpConfiguration")
    private Configuration distCpConfiguration;

    @Inject
    private S3Service s3Service;

    private HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder();

    @PostConstruct
    public void postConstruct() {
        pathBuilder = new HdfsToS3PathBuilder(useEmr);
    }

    @Override
    public String exploreS3FilePath(String inputFile) {
        try {
            URI uri = new URI(inputFile);
            String inputPath = uri.getPath();
            String s3FilePath = pathBuilder.exploreS3FilePath(inputPath, s3Bucket);
            String s3FileKey = s3FilePath.substring(pathBuilder.getS3BucketDir(s3Bucket).length() + 1);
            if (StringUtils.isNotBlank(s3FileKey) && s3Service.objectExist(s3Bucket, s3FileKey)) {
                log.info("Import from S3, bucket=" + s3Bucket + " file path=" + s3FilePath);
                return s3FilePath;
            }
        } catch (Exception ex) {
            log.warn("Failed to build path inputFile=" + inputFile + " Error=" + ex.getMessage());
        }
        log.info("Import from Hdfs, file path=" + inputFile);
        return inputFile;
    }

    @Override
    public List<String> getFilesForDir(String prefix, HdfsFilenameFilter fileFilter) {
        final String delimiter = "/";
        try {
            URI uri = new URI(prefix);
            String inputPath = uri.getPath();
            if (inputPath.startsWith(delimiter)) {
                inputPath = inputPath.substring(1);
            }
            prefix = inputPath;
            if (!prefix.endsWith(delimiter)) {
                prefix = prefix + delimiter;
            }
            List<String> files = s3Service.getFilesForDir(s3Bucket, prefix);
            return files.stream().filter(fileFilter::accept)
                    .map(file -> pathBuilder.getS3BucketDir(s3Bucket) + "/" + file)
                    .collect(Collectors.toList());
        } catch (Exception ex) {
            log.warn("Failed to get files for prefix=" + prefix + " Error=" + ex.getMessage());
        }
        return Collections.emptyList();
    }

    @Override
    public String getS3Bucket() {
        return s3Bucket;
    }

    @Override
    public String getPodId() {
        return podId;
    }

    @Override
    public InputStream getS3FileInputStream(String key) {
        InputStream in = s3Service.readObjectAsStream(getS3Bucket(), key);
        return in;
    }

    @Override
    public InputStream getS3FileInputStream(String bucketName, String key) {
        InputStream in = s3Service.readObjectAsStream(bucketName, key);
        return in;
    }

    @Override
    public String getS3FsProtocol() {
        return pathBuilder.getProtocol();
    }

}

package com.latticeengines.app.exposed.service.impl;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;

@Component
public class ImportFromS3ServiceImpl implements ImportFromS3Service {

    private static final Logger log = LoggerFactory.getLogger(ImportFromS3ServiceImpl.class);

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Value("${camille.zk.pod.id:Default}")
    private String podId;

    @Resource(name = "distCpConfiguration")
    private Configuration distCpConfiguration;

    @Inject
    private S3Service s3Service;

    protected HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder();

    @Override
    public String exploreS3FilePath(String inputFile, String customer) {
        CustomerSpace space = CustomerSpace.parse(customer);
        try {
            URI uri = new URI(inputFile);
            String inputPath = uri.getPath();
            String s3FilePath = pathBuilder.exploreS3FilePath(inputPath, podId, space.toString(), space.getTenantId(),
                    s3Bucket);
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
            return files.stream().filter(file -> fileFilter.accept(file))
                    .map(file -> pathBuilder.getS3BucketDir(s3Bucket) + "/" + file).collect(Collectors.toList());
        } catch (

        Exception ex) {
            log.warn("Failed to get files for prefix=" + prefix + " Error=" + ex.getMessage());
        }
        return Collections.emptyList();
    }

    @Override
    public String getS3Bucket() {
        return s3Bucket;
    }
}

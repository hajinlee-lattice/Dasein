package com.latticeengines.app.exposed.service.impl;

import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.yarn.configuration.ConfigurationUtils;

import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
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
        if (Boolean.TRUE.equals(useEmr)) {
            pathBuilder.setProtocol("s3a");
        }
    }

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
            return files.stream().filter(fileFilter::accept)
                    .map(file -> pathBuilder.getS3BucketDir(s3Bucket) + "/" + file).collect(Collectors.toList());
        } catch (Exception ex) {
            log.warn("Failed to get files for prefix=" + prefix + " Error=" + ex.getMessage());
        }
        return Collections.emptyList();
    }

    @Override
    public void importTable(String customer, Table table, String queueName) {
        CustomerSpace space = CustomerSpace.parse(customer);
        List<Extract> extracts = table.getExtracts();
        if (CollectionUtils.isNotEmpty(extracts)) {
            for (Extract extract : extracts) {
                if (StringUtils.isNotBlank(extract.getPath())) {
                    String hdfsPath = pathBuilder.getFullPath(extract.getPath());
                    try {
                        if (!HdfsUtils.fileExists(distCpConfiguration, hdfsPath)) {
                            String s3Dir = pathBuilder.convertAtlasTableDir(hdfsPath, podId, space.getTenantId(),
                                    s3Bucket);
                            log.info("Hdfs file does not exist, copy from S3. " +
                                    "file=" + hdfsPath + " s3Dir=" + s3Dir + " tenant=" + customer);
                            Configuration hadoopConfiguration = createConfiguration(space.getTenantId(),
                                    table.getName());
                            HdfsUtils.distcp(hadoopConfiguration, s3Dir, hdfsPath, queueName);
                        }
                    } catch (Exception ex) {
                        log.error("Failed to copy from S3, file=" + hdfsPath + " tenant=" + customer, ex);
                        throw new RuntimeException("Failed to copy from S3, file=" + hdfsPath);
                    }
                }
            }
        }

    }

    private Configuration createConfiguration(String tenantId, String tableName) {
        Properties properties = new Properties();
        Configuration hadoopConfiguration = ConfigurationUtils.createFrom(distCpConfiguration, properties);
        String jobName = StringUtils.isNotBlank(tableName) ? tenantId + "~" + tableName : tenantId;
        hadoopConfiguration.set(JobContext.JOB_NAME, jobName);
        return hadoopConfiguration;
    }

    @Override
    public void importFile(String tenantId, String s3Path, String hdfsPath, String queueName) {
        if (StringUtils.isNotBlank(hdfsPath)) {
            CustomerSpace space = CustomerSpace.parse(tenantId);
            try {
                if (!HdfsUtils.fileExists(distCpConfiguration, hdfsPath)) {
                    log.info("Hdfs file does not exist, copy from S3. file=" + hdfsPath);
                    Configuration hadoopConfiguration = createConfiguration(space.getTenantId(),
                            FilenameUtils.getName(hdfsPath));
                    HdfsUtils.distcp(hadoopConfiguration, s3Path, hdfsPath, queueName);
                }
            } catch (Exception ex) {
                log.error("Failed to copy from S3, file=" + hdfsPath, ex);
                throw new RuntimeException("Failed to copy from S3, file=" + hdfsPath);
            }
        }

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

}

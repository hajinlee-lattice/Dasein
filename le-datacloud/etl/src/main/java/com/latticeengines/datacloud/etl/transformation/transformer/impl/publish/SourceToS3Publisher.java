package com.latticeengines.datacloud.etl.transformation.transformer.impl.publish;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.EXPIRE_DAYS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.S3_TO_GLACIER_DAYS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.SERVICE_TENANT;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SOURCE_TO_S3_PUBLISHER;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.configuration.ConfigurationUtils;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.amazonaws.util.IOUtils;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.PurgeStrategyUtils;
import com.latticeengines.datacloud.core.util.RequestContext;
import com.latticeengines.datacloud.etl.purge.entitymgr.PurgeStrategyEntityMgr;
import com.latticeengines.datacloud.etl.service.SourceHdfsS3TransferService;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.AbstractTransformer;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@Component(SourceToS3Publisher.TRANSFORMER_NAME)
public class SourceToS3Publisher extends AbstractTransformer<TransformerConfig> {
    private static final Logger log = LoggerFactory.getLogger(SourceToS3Publisher.class);

    public static final String TRANSFORMER_NAME = TRANSFORMER_SOURCE_TO_S3_PUBLISHER;

    @Resource(name = "distCpConfiguration")
    private Configuration distCpConfiguration;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Inject
    private PurgeStrategyEntityMgr purgeStrategyEntityMgr;

    @Inject
    private EMREnvService emrEnvService;

    @Inject
    private S3Service s3Service;

    @Inject
    private SourceHdfsS3TransferService sourceHdfsS3TransferService;

    @Value("${datacloud.collection.s3bucket}")
    private String s3Bucket;

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected boolean validateConfig(TransformerConfig config, List<String> sourceNames) {
        if (!(config.validate(sourceNames))) {
            RequestContext.logError("Validation fails due to base source name is empty.");
            return false;
        }
        return true;
    }

    @Override
    protected boolean transformInternal(TransformationProgress progress, String workflowDir, TransformStep step) {
        try {
            for (int i = 0; i < step.getBaseSources().length; i++) {
                Source source = step.getBaseSources()[i];
                String version = step.getBaseVersions().get(i);
                List<Pair<String, String>> tags = getPurgeStrategyTags(source);
                sourceHdfsS3TransferService.transfer(true, source, version, tags, false, false);
                // _CURRENT_VERSION file should not be tagged with purge
                // strategy, remove tags on _CURRENT_VERSION
                String versionPath = hdfsPathBuilder.constructVersionFile(source).toString();
                log.info("ZDD: _CURRENT_VERSION path=" + versionPath);
                if (!(source instanceof TableSource)) {
                    s3Service.deleteObjectTags(s3Bucket, versionPath);
                }

//                String sourceName = step.getBaseSources()[i].getSourceName();
//                Source source = step.getBaseSources()[i];
//
//                String schemaPath = (source instanceof DerivedSource) ? getBaseSourceSchemaDir(step, i) : null;
//                String dataPath = getSourceHdfsDir(step, i);
//                String versionFilePath = getBaseSourceVersionFilePath(step, i);
//
//                List<Pair<String, String>> tags = getPurgeStrategyTags(source);
//
//                List<String> files = getDirFiles(dataPath);
//                copyAndValidate(sourceName, dataPath, files, true);
//                cleanupS3Path(dataPath + "_$folder$");
//                objectTagging(tags, dataPath, files);
//
//                if (schemaPath != null && HdfsUtils.fileExists(yarnConfiguration, schemaPath)) {
//                    files = getDirFiles(schemaPath);
//                    copyAndValidate(sourceName, schemaPath, files, true);
//                    cleanupS3Path(schemaPath + "_$folder$");
//                    objectTagging(tags, dataPath, files);
//
//                }
//
//                if (shouldCopyVersionFile(source, versionFilePath)) {
//                    files = getDirFiles(versionFilePath);
//                    copyAndValidate(sourceName, versionFilePath, files, false);
//                }
            }
            step.setTarget(null);
            step.setCount(0L);
            return true;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    private void objectTagging(List<Pair<String, String>> tags, String prefix, List<String> files) {
        tags.forEach(tag -> log.info("Adding tag {} with value {} on objects with prefix {}", tag.getKey(),
                tag.getValue(), prefix));
        tagS3Objects(files, tags);
    }

    private List<Pair<String, String>> getPurgeStrategyTags(Source source) {
        PurgeStrategy ps = purgeStrategyEntityMgr.findStrategyBySourceAndType(source.getSourceName(),
                PurgeStrategyUtils.getSourceType(source));
        Integer glacierDays = (ps == null) || (ps.getGlacierDays() == null) ? 180 : ps.getGlacierDays();
        Integer s3Days = (ps == null) || (ps.getS3Days() == null) ? 180 : ps.getS3Days();
        return Arrays.asList(Pair.of(S3_TO_GLACIER_DAYS, s3Days.toString()),
                Pair.of(EXPIRE_DAYS, Integer.toString(s3Days + glacierDays)));

    }

    private void tagS3Objects(List<String> files, List<Pair<String, String>> tags) {
        files.forEach(s3Path -> {
            for (Pair<String, String> tag : tags) {
                try {
                    s3Service.addTagToObject(s3Bucket, s3Path, tag.getKey(), tag.getValue());
                } catch (Exception e) {
                    log.error(String.format("Failed to tag %s with tag key: %s value: %s", s3Path, tag.getKey(),
                            tag.getValue()));
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private void copyAndValidate(String sourceName, String hdfsDir, List<String> files, boolean isDir) {
        copyToS3(sourceName, hdfsDir, files, isDir);
        validateCopySuccess(hdfsDir, files);
    }

    private void copyToS3(String sourceName, String hdfsDir, List<String> files, boolean isDir) {
        String s3nDir = gets3aPath(hdfsDir);
        try {
            cleanupS3Path(hdfsDir);

            Configuration distcpConfiguration = createConfiguration(sourceName, "HdfsToS3");

            String queue = LedpQueueAssigner.getDefaultQueueNameForSubmission();
            String overwriteQueue = LedpQueueAssigner.overwriteQueueAssignment(queue,
                    emrEnvService.getYarnQueueScheme());

            log.info("Copying from {} to {}", hdfsDir, s3nDir);

            if (isDir) {
                if (!files.isEmpty()) {
                    HdfsUtils.distcp(distcpConfiguration, hdfsDir, s3nDir, overwriteQueue);
                } else {
                    throw new RuntimeException("No file exists in dir, or Dir not exist : " + hdfsDir);
                }
            } else {
                if (HdfsUtils.fileExists(yarnConfiguration, hdfsDir)) {
                    HdfsUtils.distcp(distcpConfiguration, hdfsDir, s3nDir, overwriteQueue);
                } else {
                    throw new RuntimeException("File not exist: " + hdfsDir);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Fail to copy %s to %s", hdfsDir, s3nDir), e);
        }
    }

    private void validateCopySuccess(String hdfsDir, List<String> files) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        files.forEach(file -> {
            retry.execute(ctx -> {
                try {
                    // Due to s3Client.doesObjectExist() intermittently returns
                    // false even if the object does exist on S3, add retry for
                    // checking object existence
                    if (!s3Service.objectExist(s3Bucket, file)) {
                        throw new RuntimeException(file + " wasn't successfully copied to S3 bucket " + s3Bucket);
                    }
                    validateFileSize(file);
                } catch (Exception e) {
                    throw new RuntimeException("Fail to validate files under: " + hdfsDir, e);
                }
                return true;
            });
        });
    }

    private List<String> getDirFiles(String hdfsDir) {
        try {
            List<String> hdfsFiles = HdfsUtils.onlyGetFilesForDirRecursive(yarnConfiguration, hdfsDir,
                    (HdfsFileFilter) null, false);
            if (hdfsFiles == null) {
                return Collections.emptyList();
            }
            return hdfsFiles.stream().map(hdfsFile -> hdfsFile.substring(hdfsFile.indexOf(hdfsDir)))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Fail to get file list of HDFS path: " + hdfsDir, e);
        }
    }

    private void validateFileSize(String filePath) {
        long s3Len = s3Service.getObjectMetadata(s3Bucket, filePath).getContentLength();
        long hdfsLen;
        try {
            hdfsLen = HdfsUtils.getFileStatus(yarnConfiguration, filePath).getLen();
        } catch (Exception e) {
            throw new RuntimeException("Fail to get hdfs file size:" + filePath, e);
        }
        if (s3Len != hdfsLen) {
            throw new RuntimeException(
                    String.format("File %s size not the same on hdfs: %d and s3: %d", filePath, hdfsLen, s3Len));
        }
    }

    private void cleanupS3Path(String s3nDir) {
        if (s3Service.isNonEmptyDirectory(s3Bucket, s3nDir)) {
            s3Service.cleanupPrefix(s3Bucket, s3nDir);
        }
    }

    private Configuration createConfiguration(String sourceName, String jobNameSuffix) {
        Configuration hadoopConfiguration = ConfigurationUtils.createFrom(distCpConfiguration, new Properties());
        String jobName = SERVICE_TENANT + "~" + sourceName + "~" + jobNameSuffix;
        hadoopConfiguration.set(JobContext.JOB_NAME, jobName);
        if (StringUtils.isNotEmpty(hadoopConfiguration.get("mapreduce.application.classpath"))) {
            return hadoopConfiguration;
        } else {
            Properties properties = new Properties();
            properties.setProperty("mapreduce.application.classpath",
                    "$HADOOP_CONF_DIR,$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*"
                            + ",$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*,$HADOOP_MAPRED_HOME/share/hadoop/tools/lib/*");
            return ConfigurationUtils.createFrom(hadoopConfiguration, properties);
        }
    }

    private boolean shouldCopyVersionFile(Source source, String versionFilePath) {
        if (!s3Service.objectExist(s3Bucket, versionFilePath)) {
            return true;
        }
        try {
            Date hdfsDate = HdfsPathBuilder.dateFormat.parse(hdfsSourceEntityMgr.getCurrentVersion(source));
            Date s3Date = HdfsPathBuilder.dateFormat
                    .parse(IOUtils.toString(s3Service.readObjectAsStream(s3Bucket, versionFilePath)));
            if (hdfsDate.after(s3Date)) {
                return true;
            } else {
                return false;
            }
        } catch (Exception ex) {
            throw new RuntimeException("Fail to parse current version file", ex);
        }
    }

    private String gets3aPath(String hdfsPath) {
        return "s3a://" + s3Bucket + getValidPath(hdfsPath);
    }

    private String getValidPath(String path) {
        if (!path.startsWith("/")) {
            return "/" + path;
        } else {
            return path;
        }
    }
}

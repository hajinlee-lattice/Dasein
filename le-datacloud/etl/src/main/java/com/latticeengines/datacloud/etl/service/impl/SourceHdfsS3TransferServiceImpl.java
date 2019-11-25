package com.latticeengines.datacloud.etl.service.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.SERVICE_TENANT;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
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

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.service.SourceHdfsS3TransferService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@Component("sourceHdfsS3TransferService")
public class SourceHdfsS3TransferServiceImpl implements SourceHdfsS3TransferService {

    private static final Logger log = LoggerFactory.getLogger(SourceHdfsS3TransferServiceImpl.class);

    private static final String S3_PLACE_HOLDER = "_$folder$";

    @Resource(name = "distCpConfiguration")
    private Configuration distCpConfiguration;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    protected HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private EMREnvService emrEnvService;

    @Inject
    private S3Service s3Service;

    @Inject
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Value("${datacloud.collection.s3bucket}")
    private String s3Bucket;

    @Override
    public void transfer(boolean hdfsToS3, @NotNull Source source, @NotNull String version,
            List<Pair<String, String>> tags, boolean skipEventualCheck, boolean failIfExisted) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(3, Collections.emptyList(),
                Arrays.asList(LedpException.class));
        try {
            retry.execute(ctx -> {
                try {
                    copySchema(hdfsToS3, source, version, tags, skipEventualCheck, failIfExisted);
                } catch (Exception e) {
                    log.error(String.format("Fail to copy schema file for %s @%s from %s to %s",
                            getSourceNameForLogging(source), version, hdfsToS3 ? "HDFS" : "S3 bucket " + s3Bucket,
                            hdfsToS3 ? "S3 bucket " + s3Bucket : "HDFS"), e);
                    throw e;
                }

                try {
                    copyData(hdfsToS3, source, version, tags, skipEventualCheck, failIfExisted);
                } catch (Exception e) {
                    log.error(
                            String.format("Fail to copy data files for %s @%s from %s to %s",
                                    getSourceNameForLogging(source), version,
                                    hdfsToS3 ? "HDFS" : "S3 bucket " + s3Bucket,
                                    hdfsToS3 ? "S3 bucket " + s3Bucket : "HDFS"),
                            e);
                    throw e;
                }

                try {
                    copyVersionFile(hdfsToS3, source, version, tags, skipEventualCheck);
                } catch (Exception e) {
                    log.error(String.format("Fail to check/copy %s file for %s from %s to %s",
                            HdfsPathBuilder.VERSION_FILE, getSourceNameForLogging(source),
                            hdfsToS3 ? "HDFS" : "S3 bucket " + s3Bucket, hdfsToS3 ? "S3 bucket " + s3Bucket : "HDFS"),
                            e);
                    throw e;
                }

                return true;
            });
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Fail to copy %s @%s from %s to %s", getSourceNameForLogging(source),
                    version, hdfsToS3 ? "HDFS" : "S3 bucket " + s3Bucket, hdfsToS3 ? "S3 bucket " + s3Bucket : "HDFS"),
                    e);
        }
    }

    /**
     * For source with schema file (general source), copy schema file
     *
     * @param hdfsToS3:
     *            transfer schema file from hdfs to s3, or from s3 to hdfs
     * @param source:
     *            source to transfer
     * @param version:
     *            version of source to transfer
     * @param tags:
     *            if transfer destination is s3, how to tag schema file on s3 --
     *            provide with mapping from tag names to tag values
     * @param skipEventualCheck:
     *            after transfer, whether to do verification on number of files
     *            and file size
     * @param failIfExisted:
     *            if schema file already exists at destination, whether to fail
     *            the transfer; if not to fail, existed schema file will be
     *            deleted and then re-copy
     * @throws Exception
     */
    private void copySchema(boolean hdfsToS3, Source source, String version, List<Pair<String, String>> tags,
            boolean skipEventualCheck, boolean failIfExisted) throws Exception {
        String schemaPath = getSchemaPath(source, version);
        if (!isSchemaExistedAtOrigin(hdfsToS3, schemaPath)) {
            return;
        }
        validateDestination(hdfsToS3, schemaPath, failIfExisted);
        distcp(hdfsToS3, source.getSourceName(), schemaPath);
        if (hdfsToS3) {
            tagS3Objects(schemaPath, tags);
            cleanupS3PlaceHolder(schemaPath);
        }
        if (skipEventualCheck) {
            return;
        }
        eventualCheck(hdfsToS3, schemaPath);
    }

    /**
     * Get sanity path of source schema file starting from "/Pods/..." without
     * hdfs url or s3 bucket info
     *
     * Only DerivedSource has schema file
     *
     * @param source:
     *            source to transfer
     * @param version:
     *            version of source to transfer
     * @return: sanity path of source schema file
     */
    private String getSchemaPath(Source source, String version) {
        if (!(source instanceof DerivedSource)) {
            return null;
        }
        return getSanityPath(hdfsPathBuilder.constructSchemaDir(source.getSourceName(), version).toString());
    }

    /**
     * Whether source schema file exists at origin
     *
     * @param hdfsToS3:
     *            transfer source from hdfs to s3, or from s3 to hdfs; if true,
     *            origin is hdfs, otherwise, origin is s3
     * @param schemaPath:
     *            sanity path of source schema file
     * @return: Whether source schema file exists at origin
     * @throws IOException
     */
    private boolean isSchemaExistedAtOrigin(boolean hdfsToS3, String schemaPath) throws IOException {
        if (schemaPath == null) {
            return false;
        }
        if (hdfsToS3) {
            return HdfsUtils.fileExists(yarnConfiguration, schemaPath);
        } else {
            return s3Service.isNonEmptyDirectory(s3Bucket, schemaPath);
        }
    }

    /**
     * Copy data files of source
     *
     * @param hdfsToS3:
     *            transfer data file from hdfs to s3, or from s3 to hdfs
     * @param source:
     *            source to transfer
     * @param version:
     *            version of source to transfer
     * @param tags:
     *            if transfer destination is s3, how to tag data file on s3 --
     *            provide with mapping from tag names to tag values
     * @param skipEventualCheck:
     *            after transfer, whether to do verification on number of files
     *            and file size
     * @param failIfExisted:
     *            if data file already exists at destination, whether to fail
     *            the transfer; if not to fail, existed data file will be
     *            deleted and then re-copy
     * @throws Exception
     */
    private void copyData(boolean hdfsToS3, Source source, String version, List<Pair<String, String>> tags,
            boolean skipEventualCheck, boolean failIfExisted) throws Exception {
        String dataPath = getDataPath(source, version);
        validateDestination(hdfsToS3, dataPath, failIfExisted);
        distcp(hdfsToS3, source.getSourceName(), dataPath);
        if (hdfsToS3) {
            tagS3Objects(dataPath, tags);
            cleanupS3PlaceHolder(dataPath);
        }
        if (skipEventualCheck) {
            return;
        }
        eventualCheck(hdfsToS3, dataPath);
    }

    /**
     * Get sanity path of source data file starting from "/Pods/..." without
     * hdfs url or s3 bucket info
     *
     * @param source:
     *            source to transfer
     * @param version:
     *            version of source to transfer
     * @return: sanity path of source data file
     */
    private String getDataPath(Source source, String version) {
        if (source instanceof TableSource) {
            Table table = ((TableSource) source).getTable();
            if (table.getExtracts().size() > 1) {
                throw new IllegalArgumentException("Can only handle single extract table.");
            }
            return getSanityPath(table.getExtracts().get(0).getPath());
        }
        return getSanityPath(hdfsPathBuilder.constructTransformationSourceDir(source, version).toString());
    }

    /**
     * Copy _CURRENT_VERSION file of source
     *
     * @param hdfsToS3:
     *            transfer _CURRENT_VERSION file from hdfs to s3, or from s3 to
     *            hdfs
     * @param source:
     *            source to transfer
     * @param version:
     *            version of source to transfer
     * @param tags:
     *            if transfer destination is s3, how to tag _CURRENT_VERSION
     *            file on s3 -- provide with mapping from tag names to tag
     *            values
     * @param skipEventualCheck:
     *            after transfer, whether to do verification on number of files
     *            and file size
     * @throws Exception
     */
    private void copyVersionFile(boolean hdfsToS3, Source source, String version, List<Pair<String, String>> tags,
            boolean skipEventualCheck) throws Exception {
        String versionFilePath = getVersionFilePath(source);
        if (!shouldCopyVersionFile(hdfsToS3, source, version, versionFilePath)) {
            return;
        }
        validateDestination(hdfsToS3, versionFilePath, false);
        distcp(hdfsToS3, source.getSourceName(), versionFilePath);
        if (hdfsToS3) {
            tagS3Objects(versionFilePath, tags);
            cleanupS3PlaceHolder(versionFilePath);
        }
        if (skipEventualCheck) {
            return;
        }
        eventualCheck(hdfsToS3, versionFilePath);
    }

    /**
     * Get sanity path of source _CURRENT_VERSION file starting from "/Pods/..."
     * without hdfs url or s3 bucket info
     *
     * @param source:
     *            source to transfer
     * @return: sanity path of source _CURRENT_VERSION file
     */
    private String getVersionFilePath(Source source) {
        if (!(source instanceof DerivedSource || source instanceof IngestionSource)) {
            return null;
        }
        return getSanityPath(hdfsPathBuilder.constructVersionFile(source).toString());
    }

    /**
     * Compare version to transfer and current version at destination; If
     * version to transfer is newer than current version at destination,
     * transfer _CURRENT_VERSION file, otherwise, do not transfer
     *
     * @param hdfsToS3:
     *            transfer _CURRENT_VERSION file from hdfs to s3, or from s3 to
     *            hdfs
     * @param source:
     *            source to transfer
     * @param version:
     *            version of source to transfer
     * @param versionFilePath:
     *            sanity path of source _CURRENT_VERSION file
     * @return whether to copy source _CURRENT_VERSION file
     * @throws IOException
     */
    private boolean shouldCopyVersionFile(boolean hdfsToS3, Source source, String version, String versionFilePath)
            throws IOException {
        if (versionFilePath == null) {
            return false;
        }
        if (!s3Service.objectExist(s3Bucket, versionFilePath)) {
            return true;
        }
        String destinationVersion = hdfsToS3
                ? IOUtils.toString(s3Service.readObjectAsStream(s3Bucket, versionFilePath), Charset.defaultCharset())
                : hdfsSourceEntityMgr.getCurrentVersion(source);
        return version.compareTo(destinationVersion) > 0;
    }

    /**
     * Check whether the path to transfer exists at destination
     *
     * @param hdfsToS3:
     *            transfer source from hdfs to s3, or from s3 to hdfs; if true,
     *            destination is s3, otherwise, destination is hdfs
     * @param sanityPath:
     *            sanity path to transfer
     * @param failIfExisted:
     *            if path already exists at destination, whether to fail the
     *            transfer; if not to fail, existed path will be deleted and
     *            then re-copy
     * @throws IOException
     */
    private void validateDestination(boolean hdfsToS3, String sanityPath, boolean failIfExisted) throws IOException {
        boolean destinationExisted = hdfsToS3 ? s3Service.isNonEmptyDirectory(s3Bucket, sanityPath) : HdfsUtils.fileExists(yarnConfiguration, sanityPath);
        if (!destinationExisted) {
            return;
        }
        if (failIfExisted) {
            throw new LedpException(LedpCode.LEDP_25043,
                    new String[] { sanityPath, hdfsToS3 ? "S3 bucket " + s3Bucket : "HDFS" });
        }
        if (hdfsToS3) {
            s3Service.cleanupPrefix(s3Bucket, sanityPath);
        } else {
            HdfsUtils.rmdir(yarnConfiguration, sanityPath);
        }
    }

    /**
     * Run distcp to copy specified path
     *
     * @param hdfsToS3:
     *            transfer specified path from hdfs to s3, or from s3 to hdfs
     * @param sourceName:
     *            source to transfer
     * @param sanityPath:
     *            sanity path to transfer
     * @throws Exception
     */
    private void distcp(boolean hdfsToS3, String sourceName, String sanityPath) throws Exception {
        Configuration distcpConfig = createDistcpConfig(hdfsToS3, sourceName);
        String queue = LedpQueueAssigner.getDefaultQueueNameForSubmission();
        String overwriteQueue = LedpQueueAssigner.overwriteQueueAssignment(queue, emrEnvService.getYarnQueueScheme());
        String s3Path = getS3aPath(sanityPath);

        if (hdfsToS3) {
            log.info("Copying from HDFS {} to S3 {}", sanityPath, s3Path);
            HdfsUtils.distcp(distcpConfig, sanityPath, s3Path, overwriteQueue);
        } else {
            log.info("Copying from S3 {} to HDFS {}", s3Path, sanityPath);
            HdfsUtils.distcp(distcpConfig, s3Path, sanityPath, overwriteQueue);
        }
    }

    /**
     * Create distcp configuration
     *
     * @param hdfsToS3:
     *            transfer source from hdfs to s3, or from s3 to hdfs
     * @param sourceName:
     *            source to transfer
     * @return: distcp configuration
     */
    private Configuration createDistcpConfig(boolean hdfsToS3, String sourceName) {
        Configuration hadoopConfiguration = ConfigurationUtils.createFrom(distCpConfiguration, new Properties());
        String jobName = SERVICE_TENANT + "~" + sourceName + "~" + (hdfsToS3 ? "HdfsToS3" : "S3ToHdfs");
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

    /**
     * Tag s3 objects
     *
     * @param prefix:
     *            s3 prefix
     * @param tags:
     *            mapping of tag names to tag values
     * @throws IOException
     */
    private void tagS3Objects(String prefix, List<Pair<String, String>> tags) throws IOException {
        if (CollectionUtils.isEmpty(tags)) {
            return;
        }
        tags.forEach(tag -> log.info("Adding tag {} with value {} on objects with prefix {}", tag.getKey(),
                tag.getValue(), prefix));
        List<String> objects = getFileList(true, prefix);
        objects.forEach(object -> {
            for (Pair<String, String> tag : tags) {
                s3Service.addTagToObject(s3Bucket, object, tag.getKey(), tag.getValue());
            }
        });
    }

    /**
     * When transfer folder from hdfs to s3, aws sdk creates some place holder
     * object which is of no use. Delete it.
     *
     * @param sanityPath:
     *            sanity path to transfer
     */
    private void cleanupS3PlaceHolder(String sanityPath) {
        String s3Prefix = sanityPath + S3_PLACE_HOLDER;
        if (s3Service.isNonEmptyDirectory(s3Bucket, s3Prefix)) {
            s3Service.cleanupPrefix(s3Bucket, s3Prefix);
        }
    }

    /**
     * After transfer is complete, verify whether number of files and each file
     * size at destination are same as that at origin
     *
     * @param hdfsToS3:
     *            transfer sanity path from hdfs to s3, or from s3 to hdfs; if
     *            true, origin is hdfs and destination is s3, otherwise, origin
     *            is s3 and destination is hdfs
     * @param sanityPath:
     *            sanity path to transfer
     * @throws IOException
     */
    private void eventualCheck(boolean hdfsToS3, String sanityPath) throws IOException {
        log.info(String.format("Verifying files under %s on %s", sanityPath,
                hdfsToS3 ? "S3 bucket " + s3Bucket : "HDFS"));
        List<String> files = getFileList(hdfsToS3, sanityPath);
        for (String file : files) {
            boolean destinationExisted = hdfsToS3 ? s3Service.objectExist(s3Bucket, file)
                    : HdfsUtils.fileExists(yarnConfiguration, file);
            if (!destinationExisted) {
                throw new RuntimeException(
                        String.format("%s doesn't exist on %s", file,
                                hdfsToS3 ? "S3 bucket " + s3Bucket : "HDFS"));
            }
            long s3Size = s3Service.getObjectMetadata(s3Bucket, file).getContentLength();
            long hdfsSize = HdfsUtils.getFileStatus(yarnConfiguration, file).getLen();
            if (s3Size != hdfsSize) {
                throw new RuntimeException(
                        String.format("File size of %s is different between that on hdfs(%d) and s3(%d)", file,
                                hdfsSize, s3Size));
            }
        }
    }

    private String getSanityPath(String path) {
        if (path.endsWith(".avro")) {
            path = path.substring(0, path.lastIndexOf("/"));
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        if (path.startsWith("s3a://" + s3Bucket)) {
            path = path.substring(("s3a://" + s3Bucket).length());
        }
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return path;
    }

    private String getS3aPath(String sanityPath) {
        return "s3a://" + s3Bucket + sanityPath;
    }

    /**
     * Get file list under sanity path on hdfs/s3; only return files, no
     * sub-folders; no recursive search
     *
     * @param isHdfs:
     *            sanity path is on hdfs or s3
     * @param sanityPath:
     *            sanity path under which to get file list
     * @return
     * @throws IOException
     */
    private List<String> getFileList(boolean isHdfs, String sanityPath) throws IOException {
        if (isHdfs) {
            List<String> files = HdfsUtils.onlyGetFilesForDir(yarnConfiguration, sanityPath, (HdfsFileFilter) null);
            if (files == null) {
                return Collections.emptyList();
            } else {
                return files.stream().map(file -> file.substring(file.indexOf(sanityPath))).collect(Collectors.toList());
            }
        } else {
            List<String> files = s3Service.getFilesForDir(s3Bucket, sanityPath);
            if (files == null) {
                return Collections.emptyList();
            } else {
                return files.stream().map(file -> getSanityPath(file)).collect(Collectors.toList());
            }
        }
    }

    @VisibleForTesting
    String getSourceNameForLogging(Source source) {
        String sourceType = source instanceof IngestionSource ? IngestionSource.class.getSimpleName()
                : (source instanceof TableSource ? TableSource.class.getSimpleName()
                        : GeneralSource.class.getSimpleName());
        return sourceType + " " + source.getSourceName();
    }

}

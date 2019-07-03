package com.latticeengines.datacloud.etl.transformation.transformer.impl.publish;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.SERVICE_TENANT;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SOURCE_TO_S3_PUBLISHER;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.configuration.ConfigurationUtils;
import org.springframework.stereotype.Component;

import com.amazonaws.util.IOUtils;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.RequestContext;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.AbstractTransformer;
import com.latticeengines.domain.exposed.camille.Path;
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
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Inject
    private EMREnvService emrEnvService;

    @Inject
    private S3Service s3Service;

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

            int counter = 0;

            do {
                String sourceName = step.getBaseSources()[counter].getSourceName();
                Source source = step.getBaseSources()[counter];

                String hdfsSchemaDir = null;
                String hdfsSnapshotDir = null;
                String hdfsVersionFilePath = null;
                String IngestionVerDir = null;
                if (sourceName.contains("Ingestion")) {
                    Path ingestionDir = hdfsPathBuilder
                            .constructIngestionDir(sourceName.substring(sourceName.lastIndexOf("_") + 1));

                    hdfsVersionFilePath = ingestionDir.append(HdfsPathBuilder.VERSION_FILE).toString();
                    IngestionVerDir = ingestionDir.append(step.getBaseVersions().get(counter)).toString();
                } else {
                    hdfsSnapshotDir = getSourceHdfsDir(step, counter);
                    try {
                        hdfsSchemaDir = getBaseSourceSchemaDir(step, counter);
                    } catch (Exception e) {

                    }
                    hdfsVersionFilePath = getBaseSourceVersionFilePath(step, counter);
                }

                if (hdfsSnapshotDir != null) {
                    copyAndValidate(sourceName, hdfsSnapshotDir, true);
                }
                if (hdfsSchemaDir != null && HdfsUtils.fileExists(distCpConfiguration, hdfsSchemaDir)) {
                    copyAndValidate(sourceName, hdfsSchemaDir, true);
                }
                if (IngestionVerDir != null && HdfsUtils.fileExists(distCpConfiguration, IngestionVerDir)) {
                    copyAndValidate(sourceName, IngestionVerDir, true);
                }

                if (hdfsVersionFilePath != null && (!s3Service.objectExist(s3Bucket, hdfsVersionFilePath)
                        || isNewerVerFileHdfs(source, hdfsVersionFilePath))) {
                    copyAndValidate(sourceName, hdfsVersionFilePath, true);
                }
                counter++;
            } while (counter < step.getBaseSources().length);
            step.setTarget(null);
            step.setCount(0L);
            return true;

        } catch (Exception e) {
            log.error(e.getMessage());
            return false;
        }
    }

    private void copyAndValidate(String sourceName, String hdfsDir, boolean isDir) {
        String s3Prefix = gets3nPath(hdfsDir);
        copyToS3(sourceName, hdfsDir, s3Prefix, isDir);
        validateCopySuccess(hdfsDir);
    }

    private void copyToS3(String sourceName, String hdfsDir, String s3nDir, boolean isDir) {
        try {
            cleanupS3Path(hdfsDir);

            Configuration distcpConfiguration = createConfiguration(sourceName, "HdfsToS3");

            String queue = LedpQueueAssigner.getDefaultQueueNameForSubmission();
            String overwriteQueue = LedpQueueAssigner.overwriteQueueAssignment(queue,
                    emrEnvService.getYarnQueueScheme());

            log.info("Copying from {} to {}", hdfsDir, s3nDir);

            if (isDir) {
                if (!HdfsUtils.onlyGetFilesForDirRecursive(distcpConfiguration, hdfsDir, (HdfsFileFilter) null, false)
                        .isEmpty()) {
                    HdfsUtils.distcp(distcpConfiguration, hdfsDir, s3nDir, overwriteQueue);
                } else {
                    throw new RuntimeException("No file exists in dir, or Dir not exist : " + hdfsDir);
                }
            } else {
                if (HdfsUtils.fileExists(distcpConfiguration, hdfsDir)) {
                    HdfsUtils.distcp(distcpConfiguration, hdfsDir, s3nDir, overwriteQueue);
                } else {
                    throw new RuntimeException("File not exist: " + hdfsDir);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Fail to copy %s to %s", hdfsDir, s3nDir), e);
        }
    }

    private void validateCopySuccess(String hdfsDir) {
        try {

            List<String> files = HdfsUtils.onlyGetFilesForDirRecursive(distCpConfiguration, hdfsDir,
                    (HdfsFileFilter) null, false);

            for (String key : files) {
                String filepath = key.substring(key.indexOf(hdfsDir));
                s3Service.objectExist(s3Bucket, filepath);
            }
        } catch (Exception e) {
            throw new RuntimeException("Fail to validate copy of " + hdfsDir, e);
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

    private boolean isNewerVerFileHdfs(Source source, String hdfsDir) throws IOException, ParseException {
        if (s3Service.objectExist(s3Bucket, hdfsDir)) {
            Date hdfsDate = hdfsPathBuilder.dateFormat.parse(hdfsSourceEntityMgr.getCurrentVersion(source));
            Date s3Date = hdfsPathBuilder.dateFormat.parse(IOUtils.toString(s3Service.readObjectAsStream(s3Bucket, hdfsDir)));
            if (hdfsDate.after(s3Date)) {
                return true;
            }
        }
        return false;
    }

    private String gets3nPath(String hdfsPath) {
        return "s3n://" + s3Bucket + getValidPath(hdfsPath);
    }

    private String getValidPath(String path) {
        if (!path.startsWith("/")) {
            return "/" + path;
        } else {
            return path;
        }
    }
}


package com.latticeengines.datacloud.etl.transformation.transformer.impl.publish;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SOURCE_TO_S3_PUBLISHER;

import java.util.List;
import java.util.Properties;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.codehaus.plexus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.configuration.ConfigurationUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.util.RequestContext;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.AbstractTransformer;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@Component(SourceToS3Publisher.TRANSFORMER_NAME)
public class SourceToS3Publisher extends AbstractTransformer<TransformerConfig> {
    private static final Logger log = LoggerFactory.getLogger(SourceToS3Publisher.class);

    public static final CustomerSpace TEST_CUSTOMER = CustomerSpace.parse("SourceToS3Publisher");
    public static final String TRANSFORMER_NAME = TRANSFORMER_SOURCE_TO_S3_PUBLISHER;

    @Resource(name = "distCpConfiguration")
    private Configuration distCpConfiguration;

    @Inject
    private EMREnvService emrEnvService;

    @Autowired
    private S3Service s3Service;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    private String name;

    private String tenantId = TEST_CUSTOMER.getTenantId();


    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected boolean validateConfig(TransformerConfig config, List<String> sourceNames) {
        if (!(config.validate(sourceNames))) {
            RequestContext.logError("SourceNames validation fail.");
            return false;
        }
        return true;
    }


    @Override
    protected boolean transformInternal(TransformationProgress progress, String workflowDir, TransformStep step) {
        try {
            String hdfsSnapshotDir = getSourceHdfsDir(step, 0);
            String hdfsSchemaDir = getBaseSourceSchemaDir(step, 0);
            String hdfsVersionFilePath = getBaseSourceVersionFilePath(step, 0);
            String s3SnapshotPrefix = gets3nPath(s3Bucket, hdfsSnapshotDir);
            String s3SchemaPrefix = gets3nPath(s3Bucket, hdfsSchemaDir);
            String s3VersionFilePrefix = gets3nPath(s3Bucket, hdfsVersionFilePath);

            copyToS3(step, hdfsSnapshotDir, s3SnapshotPrefix, hdfsSchemaDir, s3SchemaPrefix, hdfsVersionFilePath,
                    s3VersionFilePrefix);


            step.setTarget(null);
            step.setCount(0L);
            return true;

        } catch (Exception e) {
            log.error("Failed to transform data", e);
            return false;
        }
    }



    private void copyToS3(TransformStep step, String hdfsSnapshotDir, String s3nSnapshotDir, String hdfsSchemaDir,
            String s3SchemaDir, String hdfsVersionFilePath, String s3VersionFilePrefix) {
        try {

            cleanupPrefix(hdfsSnapshotDir, hdfsSchemaDir, hdfsVersionFilePath);

            String subFolder = "HdfsToS3";
            Configuration distcpConfiguration = createConfiguration(step, subFolder);

            Properties properties = new Properties();
            properties.setProperty("mapreduce.application.classpath",
                    "$HADOOP_CONF_DIR,$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*"
                            + ",$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*,$HADOOP_MAPRED_HOME/share/hadoop/tools/lib/*");

            String queue = LedpQueueAssigner.getDefaultQueueNameForSubmission();
            String overwriteQueue = LedpQueueAssigner.overwriteQueueAssignment(queue,
                    emrEnvService.getYarnQueueScheme());

            log.info("hdfsPath: " + hdfsSnapshotDir);
            log.info("s3nPath: " + s3nSnapshotDir);

            HdfsUtils.distcp(ConfigurationUtils.createFrom(distcpConfiguration, properties), hdfsSnapshotDir,
                    s3nSnapshotDir,
                    overwriteQueue);
            if (StringUtils.isNotEmpty(hdfsSchemaDir)) {
                HdfsUtils.distcp(ConfigurationUtils.createFrom(distcpConfiguration, properties), hdfsSchemaDir,
                        s3SchemaDir, overwriteQueue);
            }
            if (StringUtils.isNotEmpty(hdfsVersionFilePath)) {
                HdfsUtils.distcp(ConfigurationUtils.createFrom(distcpConfiguration, properties), hdfsVersionFilePath,
                        s3VersionFilePrefix, overwriteQueue);
            }
        } catch (Exception e) {
            throw new RuntimeException("Cannot copy hdfs Dir to s3!" + e.getMessage());
        }

    }

    private void cleanupPrefix(String s3nSnapshotDir, String s3SchemaDir, String s3VersionFilePrefix) {
        if (s3Service.isNonEmptyDirectory(s3Bucket, s3nSnapshotDir)) {
            s3Service.cleanupPrefix(s3Bucket, s3nSnapshotDir);
        }
        if (s3Service.isNonEmptyDirectory(s3Bucket, s3SchemaDir)) {
            s3Service.cleanupPrefix(s3Bucket, s3SchemaDir);
        }
        if (s3Service.isNonEmptyDirectory(s3Bucket, s3VersionFilePrefix)) {
            s3Service.cleanupPrefix(s3Bucket, s3VersionFilePrefix);
        }
    }

    private Configuration createConfiguration(TransformStep step, String jobNameSuffix) {
        name = step.getTarget().getSourceName();
        Configuration hadoopConfiguration = ConfigurationUtils.createFrom(distCpConfiguration, new Properties());
        String jobName = tenantId + "~" + name + "~" + jobNameSuffix;
        hadoopConfiguration.set(JobContext.JOB_NAME, jobName);
        return hadoopConfiguration;
    }


    private String gets3nPath(String bucket, String hdfsPath) {
        return "s3n://" + bucket + getValidPath(hdfsPath);
    }

    private String getValidPath(String path) {
        if (!path.startsWith("/")) {
            return "/" + path;
        } else {
            return path;
        }
    }
}

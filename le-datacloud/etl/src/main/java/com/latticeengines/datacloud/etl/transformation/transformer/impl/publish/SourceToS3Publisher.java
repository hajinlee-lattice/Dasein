package com.latticeengines.datacloud.etl.transformation.transformer.impl.publish;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.TRANSFORMER_SOURCE_TO_S3_PUBLISHER;

import java.util.List;
import java.util.Properties;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
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
    public static final CustomerSpace TEST_CUSTOMER = CustomerSpace.parse("SourceToS3Publisher");

    public static final String TRANSFORMER_NAME = TRANSFORMER_SOURCE_TO_S3_PUBLISHER;

    private static final Logger log = LoggerFactory.getLogger(SourceToS3Publisher.class);

    private String tenantId = TEST_CUSTOMER.getTenantId();;
    private String name;



    @Resource(name = "distCpConfiguration")
    private Configuration distCpConfiguration;


    @Inject
    private EMREnvService emrEnvService;


    @Autowired
    private S3Service s3Service;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;


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
            String hdfsDir = getSourceHdfsDir(step, 0);
            String s3Prefix = gets3nPath(s3Bucket, hdfsDir);


            copyToS3(step, hdfsDir, s3Prefix);


            step.setTarget(null);
            step.setCount(0L);
            return true;

        } catch (Exception e) {
            log.error("Failed to transform data", e);
            return false;
        }
    }



    private void copyToS3(TransformStep step, String hdfsDirPath, String s3nDirPath) {
        try {
            if (s3Service.objectExist(s3Bucket, s3nDirPath)) {
                s3Service.cleanupPrefix(s3Bucket, s3nDirPath);
            }

            String subFolder = "HdfsToS3";
            Configuration distcpConfiguration = createConfiguration(step, subFolder);

            String queue = LedpQueueAssigner.getEaiQueueNameForSubmission();
            String overwriteQueue = LedpQueueAssigner.overwriteQueueAssignment(queue,
                    emrEnvService.getYarnQueueScheme());
            log.info("hdfsPath: " + hdfsDirPath);
            log.info("s3nPath: " + s3nDirPath);
            System.out.println("hdfsDirPath:" + hdfsDirPath);
            System.out.println("s3nDirPath:" + s3nDirPath);
            HdfsUtils.distcp(distcpConfiguration, hdfsDirPath, s3nDirPath, overwriteQueue);
        } catch (Exception e) {
            throw new RuntimeException("Cannot copy hdfs Dir to s3!" + e.getMessage());
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

package com.latticeengines.serviceflows.workflow.export;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.yarn.configuration.ConfigurationUtils;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToS3StepConfiguration;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public abstract class BaseExportToS3<T extends ExportToS3StepConfiguration> extends BaseWorkflowStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseExportToS3.class);

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    protected SourceFileProxy sourceFileProxy;

    @Resource(name = "distCpConfiguration")
    private Configuration distCpConfiguration;

    @Value("${aws.region}")
    private String awsRegion;

    @Value("${aws.default.access.key}")
    private String awsAccessKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String awsSecretKey;

    @Value("${aws.customer.s3.bucket}")
    protected String s3Bucket;

    @Value("${dataplatform.queue.scheme}")
    private String queueScheme;

    @Value("${camille.zk.pod.id:Default}")
    protected String podId;

    @Inject
    protected ModelSummaryProxy modelSummaryProxy;

    private String queueName;
    protected String customer;
    protected String tenantId;
    protected HdfsToS3PathBuilder pathBuilder;

    @Override
    public void onConfigurationInitialized() {
        String queue = LedpQueueAssigner.getEaiQueueNameForSubmission();
        queueName = LedpQueueAssigner.overwriteQueueAssignment(queue, queueScheme);

        customer = configuration.getCustomerSpace().toString();
        tenantId = configuration.getCustomerSpace().getTenantId();
        pathBuilder = new HdfsToS3PathBuilder();

    }

    @Override
    public void execute() {
        List<ExportRequest> requests = new ArrayList<>();
        buildRequests(requests);
        if (CollectionUtils.isEmpty(requests)) {
            log.info("There's no source dir found.");
            return;
        }
        log.info("Starting to export from hdfs to s3. size=" + requests.size());
        List<HdfsS3Exporter> exporters = new ArrayList<>();
        for (int i = 0; i < requests.size(); i++) {
            exporters.add(new HdfsS3Exporter(requests.get(i)));
        }
        int threadPoolSize = Math.min(5, requests.size());
        ExecutorService executorService = ThreadPoolUtils.getFixedSizeThreadPool("s3-export", threadPoolSize);
        ThreadPoolUtils.runRunnablesInParallel(executorService, exporters, (int) TimeUnit.DAYS.toMinutes(2), 10);
        log.info("Finished to export from hdfs to s3.");
    }

    protected abstract void buildRequests(List<ExportRequest> requests);

    private class HdfsS3Exporter implements Runnable {
        private String srcDir;
        private String tgtDir;
        private String tableName;

        HdfsS3Exporter(ExportRequest request) {
            this.srcDir = request.srcDir;
            this.tgtDir = request.tgtDir;
            this.tableName = request.tableName;
        }

        @Override
        public void run() {
            try (PerformanceTimer timer = new PerformanceTimer("Copying hdfs dir=" + srcDir + " to s3 dir=" + tgtDir)) {
                try {
                    Configuration hadoopConfiguration = createConfiguration();
                    HdfsUtils.distcp(hadoopConfiguration, srcDir, tgtDir, queueName);
                    if (StringUtils.isNotBlank(tableName)) {
                        registerDataUnit();
                    }

                } catch (Exception ex) {
                    String msg = String.format("Failed to copy hdfs dir=%s to s3 dir=%s for tenant=%s", srcDir, tgtDir,
                            getConfiguration().getCustomerSpace().toString());
                    log.error(msg, ex);
                    throw new RuntimeException(msg);
                }
            }
        }

        private Configuration createConfiguration() {
            Properties properties = new Properties();
            Configuration hadoopConfiguration = ConfigurationUtils.createFrom(distCpConfiguration, properties);
            String jobName = StringUtils.isNotBlank(tableName) ? tenantId + "~" + tableName : tenantId;
            hadoopConfiguration.set(JobContext.JOB_NAME, jobName);
            return hadoopConfiguration;
        }

        private void registerDataUnit() {
            String tenantId = configuration.getCustomerSpace().getTenantId();
            S3DataUnit unit = new S3DataUnit();
            unit.setTenant(tenantId);

            unit.setName(tableName);
            unit.setLinkedDir(tgtDir);
            DataUnit created = dataUnitProxy.create(configuration.getCustomerSpace().toString(), unit);
            log.info("Registered DataUnit: " + JsonUtils.pprint(created));
        }
    }

    class ExportRequest {
        String srcDir;
        String tgtDir;
        String tableName;

        public ExportRequest() {
        }

        public ExportRequest(String srcDir, String tgtDir) {
            super();
            this.srcDir = srcDir;
            this.tgtDir = tgtDir;
        }

        public ExportRequest(String srcDir, String tgtDir, String tableName) {
            super();
            this.srcDir = srcDir;
            this.tgtDir = tgtDir;
            this.tableName = tableName;
        }
    }
}

package com.latticeengines.serviceflows.workflow.export;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseExportToDynamoConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoExportConfig;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.yarn.exposed.service.JobService;

public abstract class BaseExportToDynamo<T extends BaseExportToDynamoConfiguration> extends BaseWorkflowStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseExportToDynamo.class);
    private static final Long ONE_DAY = TimeUnit.DAYS.toSeconds(1);

    @Inject
    protected JobService jobService;

    @Inject
    protected EaiProxy eaiProxy;

    @Inject
    protected DataUnitProxy dataUnitProxy;

    @Value("${aws.region}")
    protected String awsRegion;

    @Value("${aws.default.access.key}")
    protected String awsAccessKey;

    @Value("${aws.default.secret.key.encrypted}")
    protected String awsSecretKey;

    @Value("${eai.export.dynamo.num.mappers}")
    protected int numMappers;

    @Value("${eai.export.dynamo.signature}")
    protected String signature;

    @Value("${cdl.processAnalyze.skip.dynamo.publication}")
    protected boolean skipPublication;

    @Override
    public void execute() {
        List<DynamoExportConfig> configs = getExportConfigs();
        log.info("Going to export tables to dynamo: " + configs);
        List<Exporter> exporters = getExporters(configs);
        if (CollectionUtils.isEmpty(exporters)) {
            log.warn("No tables need to export, skip execution.");
            return;
        }
        int threadPoolSize = Math.min(2, configs.size());
        ExecutorService executors = ThreadPoolUtils.getFixedSizeThreadPool("dynamo-export", threadPoolSize);
        ThreadPoolUtils.runInParallel(executors, exporters, (int) TimeUnit.DAYS.toMinutes(2), 10);
    }

    protected abstract List<Exporter> getExporters(List<DynamoExportConfig> configs);

    protected boolean relinkDynamo(DynamoExportConfig config) {
        String customerSpace = configuration.getCustomerSpace().toString();
        DynamoDataUnit dataUnit = (DynamoDataUnit) dataUnitProxy.getByNameAndType(customerSpace,
                config.getLinkTableName(), DataUnit.StorageType.Dynamo);
        if (dataUnit == null) {
            log.warn("Cannot find dynamo data unit with name: " + config.getLinkTableName());
            return false;
        }
        dataUnit.setLinkedTable(config.getLinkTableName());
        dataUnit.setName(config.getTableName());
        dataUnitProxy.create(customerSpace, dataUnit);
        log.info(String.format("Relink dynamo data unit %s to %s", config.getTableName(), config.getLinkTableName()));
        return true;
    }

    protected abstract List<DynamoExportConfig> getExportConfigs();

    protected abstract class Exporter implements Runnable {

        protected final DynamoExportConfig config;

        Exporter(DynamoExportConfig config) {
            this.config = config;
        }

        @Override
        public void run() {
            try (PerformanceTimer timer = new PerformanceTimer("Upload table " + config + " to dynamo.")) {
                log.info("Uploading table " + config.getTableName() + " to dynamo.");
                HdfsToDynamoConfiguration eaiConfig = generateEaiConfig();
                RetryTemplate retry = RetryUtils.getExponentialBackoffRetryTemplate(3, 5000, 2.0, null);
                AppSubmission appSubmission = retry.execute(context -> {
                    if (context.getRetryCount() > 0) {
                        log.info("(Attempt=" + (context.getRetryCount() + 1) + ") submitting eai job.");
                    }
                    return eaiProxy.submitEaiJob(eaiConfig);
                });
                String appId = appSubmission.getApplicationIds().get(0);
                JobStatus jobStatus = jobService.waitFinalJobStatus(appId, ONE_DAY.intValue());
                if (!FinalApplicationStatus.SUCCEEDED.equals(jobStatus.getStatus())) {
                    throw new RuntimeException("Yarn application " + appId + " did not finish in SUCCEEDED status, but " //
                            + jobStatus.getStatus() + " instead.");
                }
                registerDataUnit();
            }
        }

        protected abstract HdfsToDynamoConfiguration generateEaiConfig();

        protected String getInputPath() {
            String path = config.getInputPath();
            if (path.endsWith(".avro") || path.endsWith("/")) {
                path = path.substring(0, path.lastIndexOf("/"));
            }
            return path;
        }


        protected abstract void registerDataUnit();
    }
}

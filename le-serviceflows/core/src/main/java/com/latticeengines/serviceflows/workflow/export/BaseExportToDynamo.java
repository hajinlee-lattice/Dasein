package com.latticeengines.serviceflows.workflow.export;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
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
    private JobService jobService;

    @Inject
    private EaiProxy eaiProxy;

    @Inject
    protected DataUnitProxy dataUnitProxy;

    @Value("${aws.region}")
    private String awsRegion;

    @Value("${aws.default.access.key}")
    private String awsAccessKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String awsSecretKey;

    @Value("${eai.export.dynamo.num.mappers}")
    private int numMappers;

    @Value("${eai.export.dynamo.signature}")
    protected String signature;

    @Value("${cdl.processAnalyze.skip.dynamo.publication}")
    private boolean skipPublication;

    @Override
    public void execute() {
        List<DynamoExportConfig> configs = getExportConfigs();
        if (CollectionUtils.isEmpty(configs)) {
            log.warn("No tables need to export, skip execution.");
            return;
        }
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

    protected List<Exporter> getExporters(List<DynamoExportConfig> configs) {
        List<Exporter> exporters = new ArrayList<>();
        configs.forEach(config -> {
            if (!Boolean.TRUE.equals(config.getRelink()) || !relinkDynamo(config)) {
                if (skipPublication) {
                    log.info("Skip exporting {} to DynamoDB, due to property flag.", config.getTableName());
                } else {
                    Exporter exporter = new Exporter(config);
                    exporters.add(exporter);
                }
            }
        });
        return exporters;
    }

    private boolean relinkDynamo(DynamoExportConfig config) {
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

    private List<DynamoExportConfig> getExportConfigs() {
        List<DynamoExportConfig> tables = getListObjectFromContext(configuration.getContextKey(), DynamoExportConfig.class);
        if (CollectionUtils.isEmpty(tables) && configuration.needEmptyFailed()) {
            throw new IllegalStateException("Cannot find tables to be published to dynamo.");
        }
        return tables;
    }

    protected class Exporter implements Runnable {

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

        protected String getInputPath() {
            String path = config.getInputPath();
            if (path.endsWith(".avro") || path.endsWith("/")) {
                path = path.substring(0, path.lastIndexOf("/"));
            }
            return path;
        }

        HdfsToDynamoConfiguration generateEaiConfig() {
            String tableName = config.getTableName();
            String inputPath = getInputPath();
            log.info("Found input path for table " + tableName + ": " + inputPath);

            HdfsToDynamoConfiguration eaiConfig = new HdfsToDynamoConfiguration();
            eaiConfig.setName("ExportDynamo_" + tableName);
            eaiConfig.setCustomerSpace(configuration.getCustomerSpace());
            eaiConfig.setExportDestination(ExportDestination.DYNAMO);
            eaiConfig.setExportFormat(ExportFormat.AVRO);
            eaiConfig.setExportInputPath(inputPath);
            eaiConfig.setUsingDisplayName(false);
            eaiConfig.setExportTargetPath("/tmp/path");

            String recordClass = configuration.getEntityClass().getCanonicalName();
            String recordType = configuration.getEntityClass().getSimpleName() + "_" + configuration.getDynamoSignature();
            String tenantId = CustomerSpace.shortenCustomerSpace(configuration.getCustomerSpace().toString());

            Map<String, String> properties = new HashMap<>();
            properties.put(HdfsToDynamoConfiguration.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED,
                    CipherUtils.encrypt(awsAccessKey));
            properties.put(HdfsToDynamoConfiguration.CONFIG_AWS_SECRET_KEY_ENCRYPTED,
                    CipherUtils.encrypt(awsSecretKey));
            properties.put(HdfsToDynamoConfiguration.CONFIG_ENTITY_CLASS_NAME, recordClass);
            properties.put(HdfsToDynamoConfiguration.CONFIG_REPOSITORY, configuration.getRepoName());
            properties.put(HdfsToDynamoConfiguration.CONFIG_RECORD_TYPE, recordType);
            if (configuration.needKeyPrefix()) {
                properties.put(HdfsToDynamoConfiguration.CONFIG_KEY_PREFIX, tenantId + "_" + tableName);
            }
            properties.put(HdfsToDynamoConfiguration.CONFIG_PARTITION_KEY, config.getPartitionKey());
            properties.put(HdfsToDynamoConfiguration.CONFIG_SORT_KEY, config.getSortKey());
            properties.put(HdfsToDynamoConfiguration.CONFIG_AWS_REGION, awsRegion);
            properties.put(ExportProperty.NUM_MAPPERS, String.valueOf(numMappers));
            eaiConfig.setProperties(properties);

            return eaiConfig;
        }

        void registerDataUnit() {
            String customerSpace = configuration.getCustomerSpace().toString();
            DynamoDataUnit unit = new DynamoDataUnit();
            unit.setTenant(CustomerSpace.shortenCustomerSpace(customerSpace));
            unit.setEntityClass(configuration.getEntityClass().getCanonicalName());
            if (configuration.getMigrateTable() == null || BooleanUtils.isFalse(configuration.getMigrateTable())) {
                String srcTbl = StringUtils.isNotBlank(config.getSrcTableName()) ? config.getSrcTableName()
                        : config.getTableName();
                unit.setName(srcTbl);
                if (!unit.getName().equals(config.getTableName())) {
                    unit.setLinkedTable(config.getTableName());
                }
                unit.setPartitionKey(config.getPartitionKey());
                if (StringUtils.isNotBlank(config.getSortKey())) {
                    unit.setSortKey(config.getSortKey());
                }
                unit.setSignature(configuration.getDynamoSignature());
                DataUnit created = dataUnitProxy.create(customerSpace, unit);
                log.info("Registered DataUnit: " + JsonUtils.pprint(created));
            } else {
                unit.setName(config.getTableName());
                dataUnitProxy.updateSignature(customerSpace, unit, configuration.getDynamoSignature());
                log.info("Update signature to {} for dynamo data unit with name {} and tenant {}.",
                        configuration.getDynamoSignature(), unit.getName(), customerSpace);
            }
        }
    }
}

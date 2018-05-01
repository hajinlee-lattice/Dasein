package com.latticeengines.serviceflows.workflow.export;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datafabric.GenericTableEntity;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DynamoTableConfig;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToDynamoStepConfiguration;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.yarn.exposed.service.JobService;


@Component("exportToDynamo")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportToDynamo extends BaseWorkflowStep<ExportToDynamoStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportToDynamo.class);
    private static final Long ONE_DAY = TimeUnit.DAYS.toSeconds(1);

    @Inject
    private JobService jobService;

    @Inject
    private EaiProxy eaiProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Value("${aws.region}")
    private String awsRegion;

    @Value("${aws.default.access.key}")
    private String awsAccessKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String awsSecretKey;

    @Value("${eai.export.dynamo.num.mappers}")
    private int numMappers;

    @Override
    public void execute() {
        List<DynamoTableConfig> tables = getTableNames();
        log.info("Going to export tables to dynamo: " + tables);

        List<Exporter> exporters = new ArrayList<>();
        tables.forEach(config -> {
            Exporter exporter = new Exporter(config);
            exporters.add(exporter);
        });

        int threadPoolSize = Math.min(4, tables.size());
        ExecutorService executors = ThreadPoolUtils.getFixedSizeThreadPool("dynamo-export", threadPoolSize);
        ThreadPoolUtils.runRunnablesInParallel(executors, exporters, (int) TimeUnit.DAYS.toMinutes(2), 10);
    }

    private List<DynamoTableConfig> getTableNames() {
        List<DynamoTableConfig> tables = getListObjectFromContext(TABLES_GOING_TO_DYNAMO, DynamoTableConfig.class);
        if (CollectionUtils.isEmpty(tables)) {
            throw new IllegalStateException("Cannot find tables to be published to dynamo.");
        }
        return tables;
    }

    private class Exporter implements Runnable {

        private final DynamoTableConfig config;
        Exporter(DynamoTableConfig config) {
            this.config = config;
        }

        @Override
        public void run() {
            try (PerformanceTimer timer = new PerformanceTimer("Upload table " + config + " to dynamo.")) {
                log.info("Uploading table " + config.getTableName() + " to dynamo.");
                HdfsToDynamoConfiguration eaiConfig = generateEaiConfig(config);
                AppSubmission appSubmission = eaiProxy.submitEaiJob(eaiConfig);
                String appId = appSubmission.getApplicationIds().get(0);
                JobStatus jobStatus = jobService.waitFinalJobStatus(appId, ONE_DAY.intValue());
                if (!FinalApplicationStatus.SUCCEEDED.equals(jobStatus.getStatus())) {
                    throw new RuntimeException("Yarn application " + appId + " did not finish in SUCCEEDED status, but " //
                            + jobStatus.getStatus() + " instead.");
                }
                registerDataUnit(config);
            }
        }

        private HdfsToDynamoConfiguration generateEaiConfig(DynamoTableConfig config) {
            String tableName = config.getTableName();
            String inputPath = getInputPath(tableName);
            log.info("Found input path for table " + tableName + ": " + inputPath);

            HdfsToDynamoConfiguration eaiConfig = new HdfsToDynamoConfiguration();
            eaiConfig.setName("ExportDynamo_" + tableName);
            eaiConfig.setCustomerSpace(configuration.getCustomerSpace());
            eaiConfig.setExportDestination(ExportDestination.DYNAMO);
            eaiConfig.setExportFormat(ExportFormat.AVRO);
            eaiConfig.setExportInputPath(inputPath);
            eaiConfig.setUsingDisplayName(false);
            eaiConfig.setExportTargetPath("/tmp/path");

            String recordClass = GenericTableEntity.class.getCanonicalName();
            String recordType = GenericTableEntity.class.getSimpleName() + "_" + configuration.getDynamoSignature();
            String tenantId = CustomerSpace.shortenCustomerSpace(configuration.getCustomerSpace().toString());

            Map<String, String> properties = new HashMap<>();
            properties.put(HdfsToDynamoConfiguration.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED, CipherUtils.encrypt(awsAccessKey));
            properties.put(HdfsToDynamoConfiguration.CONFIG_AWS_SECRET_KEY_ENCRYPTED, CipherUtils.encrypt(awsSecretKey));
            properties.put(HdfsToDynamoConfiguration.CONFIG_ENTITY_CLASS_NAME, recordClass);
            properties.put(HdfsToDynamoConfiguration.CONFIG_REPOSITORY, "GenericTable");
            properties.put(HdfsToDynamoConfiguration.CONFIG_RECORD_TYPE, recordType);
            properties.put(HdfsToDynamoConfiguration.CONFIG_KEY_PREFIX, tenantId + "_" + tableName);
            properties.put(HdfsToDynamoConfiguration.CONFIG_PARTITION_KEY, config.getPartitionKey());
            properties.put(HdfsToDynamoConfiguration.CONFIG_SORT_KEY, config.getSortKey());
            properties.put(HdfsToDynamoConfiguration.CONFIG_AWS_REGION, awsRegion);
            properties.put(ExportProperty.NUM_MAPPERS, String.valueOf(numMappers));
            eaiConfig.setProperties(properties);

            return eaiConfig;
        }

        private String getInputPath(String tableName) {
            Table table = metadataProxy.getTable(configuration.getCustomerSpace().toString(), tableName);
            if (table == null) {
                throw new IllegalArgumentException("Cannot find table named " + tableName);
            }
            List<Extract> extracts = table.getExtracts();
            if (CollectionUtils.isEmpty(extracts) || extracts.size() != 1) {
                throw new IllegalArgumentException("Table " + tableName + " does not have single extract");
            }
            Extract extract = extracts.get(0);
            String path = extract.getPath();
            if (path.endsWith(".avro")) {
                path = path.substring(0, path.lastIndexOf("/"));
            }
            return path;
        }

        private void registerDataUnit(DynamoTableConfig config) {
            String customerSpace = configuration.getCustomerSpace().toString();
            DynamoDataUnit unit = new DynamoDataUnit();
            unit.setTenant(CustomerSpace.shortenCustomerSpace(customerSpace));
            unit.setName(config.getTableName());
            unit.setPartitionKey(config.getPartitionKey());
            if (StringUtils.isNotBlank(config.getSortKey())){
                unit.setSortKey(config.getSortKey());
            }
            unit.setSignature(configuration.getDynamoSignature());

            DataUnit created = dataUnitProxy.create(customerSpace, unit);
            log.info("Registered DataUnit: " + JsonUtils.pprint(created));
        }
    }



}

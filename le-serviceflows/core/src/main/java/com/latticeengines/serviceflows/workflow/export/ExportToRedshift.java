package com.latticeengines.serviceflows.workflow.export;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.RedshiftDataUnit;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportToRedshiftStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.core.steps.RedshiftExportConfig;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.yarn.exposed.service.JobService;

@Component("exportToRedshift")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ExportToRedshift extends BaseWorkflowStep<ExportToRedshiftStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ExportToRedshift.class);
    private static final Long ONE_DAY = TimeUnit.DAYS.toSeconds(1);

    @Inject
    private EaiProxy eaiProxy;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private JobService jobService;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    @Override
    public void execute() {
        List<RedshiftExportConfig> configs = getExportConfigs();
        log.info("Going to export tables to redshift: " + configs);

        List<Exporter> exporters = new ArrayList<>();
        configs.forEach(config -> {
            Exporter exporter = new Exporter(config);
            exporters.add(exporter);
        });

        int threadPoolSize = Math.min(2, configs.size());
        ExecutorService executors = ThreadPoolUtils.getFixedSizeThreadPool("redshift-export", threadPoolSize);
        ThreadPoolUtils.runRunnablesInParallel(executors, exporters, (int) TimeUnit.DAYS.toMinutes(1), 10);
    }

    private List<RedshiftExportConfig> getExportConfigs() {
        List<RedshiftExportConfig> tables = getListObjectFromContext(TABLES_GOING_TO_REDSHIFT,
                RedshiftExportConfig.class);
        if (CollectionUtils.isEmpty(tables)) {
            throw new IllegalStateException("Cannot find tables to be published to redshift.");
        }
        return tables;
    }

    private class Exporter implements Runnable {

        private final RedshiftExportConfig config;

        Exporter(RedshiftExportConfig config) {
            this.config = config;
        }

        @Override
        public void run() {
            String mode = config.isUpdateMode() ? "update" : "create";
            try (PerformanceTimer timer = new PerformanceTimer(
                    "Exported table " + config.getTableName() + " to redshift in " + mode + " mode.")) {
                log.info("Exporting table " + config.getTableName() + " to redshift in " + mode + " mode.");
                ExportConfiguration eaiConfig = generateEaiConfig(config);
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

        private ExportConfiguration generateEaiConfig(RedshiftExportConfig config) {
            HdfsToRedshiftConfiguration exportConfig = new HdfsToRedshiftConfiguration();
            exportConfig.setExportFormat(ExportFormat.AVRO);
            exportConfig.setExportDestination(ExportDestination.REDSHIFT);
            exportConfig.setCleanupS3(true);
            exportConfig.setCreateNew(true);
            exportConfig.setAppend(true);

            exportConfig.setCustomerSpace(configuration.getCustomerSpace());
            exportConfig.setExportInputPath(getInputAvroGlob(config.getInputPath()));
            exportConfig.setExportTargetPath("/tmp/export");
            exportConfig.setNoSplit(true);
            if (config.isUpdateMode()) {
                exportConfig.setCreateNew(false);
                exportConfig.setAppend(false);
            } else {
                exportConfig.setCreateNew(true);
                exportConfig.setAppend(true);
            }

            String distKey = config.getDistKey();
            List<String> sortKeys = config.getSortKeys();
            RedshiftTableConfiguration.SortKeyType sortKeyType = sortKeys.size() == 1
                    ? RedshiftTableConfiguration.SortKeyType.Compound
                    : RedshiftTableConfiguration.SortKeyType.Interleaved;
            String targetTable = config.getTableName();
            RedshiftTableConfiguration redshiftTableConfig = new RedshiftTableConfiguration();
            redshiftTableConfig.setS3Bucket(s3Bucket);
            redshiftTableConfig.setDistStyle(RedshiftTableConfiguration.DistStyle.Key);
            redshiftTableConfig.setDistKey(distKey);
            redshiftTableConfig.setSortKeyType(sortKeyType);
            redshiftTableConfig.setSortKeys(sortKeys);
            redshiftTableConfig.setTableName(targetTable);
            redshiftTableConfig.setJsonPathPrefix(
                    String.format("%s/jsonpath/%s.jsonpath", RedshiftUtils.AVRO_STAGE, targetTable));
            exportConfig.setRedshiftTableConfiguration(redshiftTableConfig);
            return exportConfig;
        }

        private String getInputAvroGlob(String path) {
            if (!path.endsWith(".avro")) {
                path = path.endsWith("/") ? path + "*.avro" : path + "/*.avro";
            }
            return path;
        }

        private void registerDataUnit(RedshiftExportConfig config) {
            String customerSpace = configuration.getCustomerSpace().toString();
            RedshiftDataUnit unit = new RedshiftDataUnit();
            unit.setTenant(CustomerSpace.shortenCustomerSpace(customerSpace));
            unit.setName(config.getTableName());
            unit.setRedshiftTable(config.getTableName().toLowerCase());
            DataUnit created = dataUnitProxy.create(customerSpace, unit);
            log.info("Registered DataUnit: " + JsonUtils.pprint(created));
        }

    }

}

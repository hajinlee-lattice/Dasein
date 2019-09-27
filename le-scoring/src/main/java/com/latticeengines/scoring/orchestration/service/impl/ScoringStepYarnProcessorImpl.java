package com.latticeengines.scoring.orchestration.service.impl;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.service.DbMetadataService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.hadoop.exposed.service.ManifestService;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;
import com.latticeengines.scoring.orchestration.service.ScoringStepYarnProcessor;
import com.latticeengines.scoring.runtime.mapreduce.ScoringProperty;
import com.latticeengines.scoring.service.ScoringJobService;
import com.latticeengines.scoring.util.ScoringJobUtil;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;

//import java.util.Arrays;

@Component("scoringStepYarnProcessor")
public class ScoringStepYarnProcessorImpl implements ScoringStepYarnProcessor {

    private static final Logger log = LoggerFactory.getLogger(ScoringStepYarnProcessorImpl.class);

    @Inject
    private SqoopProxy sqoopProxy;

    @Inject
    private DbMetadataService dbMetadataService;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private JdbcTemplate scoringJdbcTemplate;

    @Inject
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Inject
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    @Inject
    private DbCreds scoringCreds;

    @Inject
    private ScoringJobService scoringJobService;

    @Inject
    private ManifestService manifestService;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Value("${scoring.output.table.sample}")
    private String targetRawTable;

    @Value("${scoring.mapper.threshold}")
    private String recordFileThreshold;

    @Value("${scoring.mapper.logdir}")
    private String scoringMapperLogDir;

    @Value("${scoring.mapper.max.input.split.size}")
    private String maxInputSplitSize;

    @Value("${scoring.mapper.min.input.split.size}")
    private String minInputSplitSize;

    @Value("${dataplatform.hdfs.stack:}")
    private String stackName;

    private static final String OUTPUT_TABLE_PREFIX = "Leads_";

    private static final String PID = "Pid";

    private static final Joiner commaJoiner = Joiner.on(", ").skipNulls();

    @Override
    @SuppressWarnings("incomplete-switch")
    public ApplicationId executeYarnStep(ScoringCommand scoringCommand, ScoringCommandStep currentStep) {
        ApplicationId appId = null;
        switch (currentStep) {
        case LOAD_DATA:
            appId = load(scoringCommand);
            break;
        case SCORE_DATA:
            appId = score(scoringCommand);
            break;
        case EXPORT_DATA:
            appId = export(scoringCommand);
            break;
        }

        return appId;
    }

    private ApplicationId load(ScoringCommand scoringCommand) {
        String table = scoringCommand.getTableName();
        String tenant = getTenant(scoringCommand);
        String targetDir = customerBaseDir + "/" + tenant + "/scoring/" + table + "/data";
        dbMetadataService.addPrimaryKeyColumn(scoringJdbcTemplate, table, PID);
        try {
            String scoringTableDir = customerBaseDir + "/" + tenant + "/scoring/" + table;
            if (HdfsUtils.fileExists(yarnConfiguration, scoringTableDir)) {
                HdfsUtils.rmdir(yarnConfiguration, scoringTableDir);
            }
        } catch (Exception e) {
            log.error(ExceptionUtils.getStackTrace(e));
            throw new LedpException(LedpCode.LEDP_00004, new String[] { targetDir });
        }

        SqoopImporter importer = new SqoopImporter.Builder() //
                .setTable(table) //
                .setTargetDir(targetDir)//
                .setDbCreds(scoringCreds) //
                .setQueue(LedpQueueAssigner.getScoringQueueNameForSubmission())//
                .setCustomer(tenant)//
                .setSplitColumn(PID)//
                .build();//
        String appIdStr = sqoopProxy.importData(importer).getApplicationIds().get(0);
        return ApplicationIdUtils.toApplicationIdObj(appIdStr);
    }

    private ApplicationId score(ScoringCommand scoringCommand) {
        Properties properties = generateCustomizedProperties(scoringCommand);
        return scoringJobService.score(properties);
    }

    private ApplicationId export(ScoringCommand scoringCommand) {
        String queue = LedpQueueAssigner.getScoringQueueNameForSubmission();
        String targetTable = createNewTable(scoringCommand);
        String tenant = getTenant(scoringCommand);
        String sourceDir = customerBaseDir + "/" + tenant + "/scoring/" + scoringCommand.getTableName() + "/scores";

        SqoopExporter exporter = new SqoopExporter.Builder() //
                .setQueue(queue)//
                .setTable(targetTable) //
                .setSourceDir(sourceDir) //
                .setDbCreds(scoringCreds) //
                .setCustomer(tenant)//
                .build();
        String appIdStr = sqoopProxy.exportData(exporter).getApplicationIds().get(0);
        ApplicationId appId = ConverterUtils.toApplicationId(appIdStr);

        saveStateBeforeFinishStep(scoringCommand, targetTable);
        return appId;
    }

    private String getTenant(ScoringCommand scoringCommand) {
        String customer = scoringCommand.getId();
        CustomerSpace customerSpace = CustomerSpace.parse(customer);
        return customerSpace.toString();
    }

    private String createNewTable(ScoringCommand scoringCommand) {
        String newTable = OUTPUT_TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
        dbMetadataService.createNewEmptyTableFromExistingOne(scoringJdbcTemplate, newTable, targetRawTable);
        return newTable;
    }

    private void saveStateBeforeFinishStep(ScoringCommand scoringCommand, String targetTable) {
        DateTime dt = new DateTime(DateTimeZone.UTC);
        ScoringCommandResult result = new ScoringCommandResult(scoringCommand.getId(), ScoringCommandStatus.NEW,
                targetTable, 0, new Timestamp(dt.getMillis()));
        scoringCommandResultEntityMgr.create(result);

        ScoringCommandState state = scoringCommandStateEntityMgr.findLastStateByScoringCommand(scoringCommand);
        state.setLeadOutputQueuePid(result.getPid());
        scoringCommandStateEntityMgr.createOrUpdate(state);
    }

    private Properties generateCustomizedProperties(ScoringCommand scoringCommand) {
        String table = scoringCommand.getTableName();
        String tenant = getTenant(scoringCommand);

        Properties properties = new Properties();
        properties.setProperty(MapReduceProperty.CUSTOMER.name(), tenant);
        properties.setProperty(MapReduceProperty.QUEUE.name(), LedpQueueAssigner.getScoringQueueNameForSubmission());
        properties.setProperty(MapReduceProperty.INPUT.name(),
                customerBaseDir + "/" + tenant + "/scoring/" + table + "/data");
        properties.setProperty(MapReduceProperty.OUTPUT.name(),
                customerBaseDir + "/" + tenant + "/scoring/" + table + "/scores");
        properties.setProperty(MapReduceProperty.MAX_INPUT_SPLIT_SIZE.name(), maxInputSplitSize);
        properties.setProperty(MapReduceProperty.MIN_INPUT_SPLIT_SIZE.name(), minInputSplitSize);
        properties.setProperty(ScoringProperty.RECORD_FILE_THRESHOLD.name(), recordFileThreshold);
        properties.setProperty(ScoringProperty.LEAD_INPUT_QUEUE_ID.name(), Long.toString(scoringCommand.getPid()));
        properties.setProperty(ScoringProperty.UNIQUE_KEY_COLUMN.name(), ScoringDaemonService.UNIQUE_KEY_COLUMN);
        properties.setProperty(ScoringProperty.TENANT_ID.name(), tenant);
        properties.setProperty(ScoringProperty.LOG_DIR.name(), scoringMapperLogDir);

        String tableName = scoringCommand.getTableName();
        List<String> modelGuids = dbMetadataService.getDistinctColumnValues(scoringJdbcTemplate, tableName,
                ScoringDaemonService.MODEL_GUID);
        List<String> cacheFiles;
        try {
            scoringJobService.syncModelsFromS3ToHdfs(tenant);
            cacheFiles = ScoringJobUtil.getCacheFiles(yarnConfiguration, manifestService.getLedpStackVersion(), //
                    manifestService.getLedsVersion());
            cacheFiles.addAll(ScoringJobUtil.findModelUrlsToLocalize(yarnConfiguration, tenant, customerBaseDir,
                    modelGuids, Boolean.FALSE));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        properties.setProperty(MapReduceProperty.CACHE_FILE_PATH.name(), commaJoiner.join(cacheFiles));
        properties.setProperty(ScoringProperty.USE_SCOREDERIVATION.name(), Boolean.FALSE.toString());
        properties.setProperty(ScoringProperty.READ_MODEL_ID_FROM_RECORD.name(), Boolean.TRUE.toString());
        return properties;
    }

    @VisibleForTesting
    void setYarnConfiguration(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }
}

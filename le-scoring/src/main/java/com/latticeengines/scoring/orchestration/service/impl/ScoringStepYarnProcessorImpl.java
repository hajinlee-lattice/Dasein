package com.latticeengines.scoring.orchestration.service.impl;

import java.sql.Timestamp;
//import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
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
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;
import com.latticeengines.scoring.orchestration.service.ScoringStepYarnProcessor;
import com.latticeengines.scoring.runtime.mapreduce.ScoringProperty;
import com.latticeengines.scoring.service.ScoringJobService;
import com.latticeengines.scoring.util.ScoringJobUtil;
import com.latticeengines.sqoop.exposed.service.SqoopJobService;

@Component("scoringStepYarnProcessor")
public class ScoringStepYarnProcessorImpl implements ScoringStepYarnProcessor {

    private static final Log log = LogFactory.getLog(ScoringStepYarnProcessorImpl.class);

//    @Autowired
//    private SqoopProxy sqoopProxy;
    
    @Autowired
    private SqoopJobService sqoopJobService;

    @Autowired
    private DbMetadataService dbMetadataService;

    @Autowired
    private Configuration yarnConfiguration;

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

    @Autowired
    private JdbcTemplate scoringJdbcTemplate;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Autowired
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    @Autowired
    private DbCreds scoringCreds;

    @Autowired
    private ScoringJobService scoringJobService;

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
            log.error(ExceptionUtils.getFullStackTrace(e));
            throw new LedpException(LedpCode.LEDP_00004, new String[] { targetDir });
        }
//         ApplicationId appId = sqoopSyncJobService.importData(table,
//         targetDir, scoringCreds,
//         LedpQueueAssigner.getScoringQueueNameForSubmission(), tenant,
//         Arrays.asList(PID), "");
//         return appId;

        SqoopImporter importer = new SqoopImporter.Builder() //
                .setTable(table) //
                .setTargetDir(targetDir)//
                .setDbCreds(scoringCreds) //
                .setQueue(LedpQueueAssigner.getScoringQueueNameForSubmission())//
                .setCustomer(tenant)//
                .setSplitColumn(PID)//
                .build();//
        return sqoopJobService.importData(importer);
    }

    private ApplicationId score(ScoringCommand scoringCommand) {
        Properties properties = generateCustomizedProperties(scoringCommand);
        ApplicationId appId = scoringJobService.score(properties);
        return appId;
    }

    private ApplicationId export(ScoringCommand scoringCommand) {
        String queue = LedpQueueAssigner.getScoringQueueNameForSubmission();
        String targetTable = createNewTable(scoringCommand);
        String tenant = getTenant(scoringCommand);
        String sourceDir = customerBaseDir + "/" + tenant + "/scoring/" + scoringCommand.getTableName() + "/scores";
//         ApplicationId appId = sqoopSyncJobService.exportData(targetTable,
//         sourceDir, scoringCreds, queue, tenant);

        SqoopExporter exporter = new SqoopExporter.Builder() //
                .setQueue(queue)//
                .setTable(targetTable) //
                .setSourceDir(sourceDir) //
                .setDbCreds(scoringCreds) //
                .setCustomer(tenant)//
                .build();
        ApplicationId appId = sqoopJobService.exportData(exporter);

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
        properties.setProperty(MapReduceProperty.INPUT.name(), customerBaseDir + "/" + tenant + "/scoring/" + table
                + "/data");
        properties.setProperty(MapReduceProperty.OUTPUT.name(), customerBaseDir + "/" + tenant + "/scoring/" + table
                + "/scores");
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
        List<String> modelUrls = ScoringJobUtil.findModelUrlsToLocalize(yarnConfiguration, tenant, customerBaseDir,
                modelGuids, Boolean.FALSE.booleanValue());
        properties.setProperty(MapReduceProperty.CACHE_FILE_PATH.name(), commaJoiner.join(modelUrls));
        properties.setProperty(ScoringProperty.USE_SCOREDERIVATION.name(), Boolean.FALSE.toString());
        return properties;
    }

    @VisibleForTesting
    void setYarnConfiguration(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }
}

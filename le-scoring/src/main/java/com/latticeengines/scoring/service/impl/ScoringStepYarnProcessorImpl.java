package com.latticeengines.scoring.service.impl;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.dataplatform.exposed.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.dataplatform.exposed.service.JobNameService;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scheduler.exposed.fairscheduler.LedpQueueAssigner;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;
import com.latticeengines.scoring.runtime.mapreduce.ScoringProperty;
import com.latticeengines.scoring.service.ScoringStepYarnProcessor;

@Component("scoringStepYarnProcessor")
public class ScoringStepYarnProcessorImpl implements ScoringStepYarnProcessor {

    @Autowired
    private SqoopSyncJobService sqoopSyncJobService;

    @Autowired
    private JobService jobService;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private JobNameService jobNameService;

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Value("${scoring.output.table.sample}")
    private String targetRawTable;

    @Value("${scoring.mapper.threshold}")
    private String leadFileThreshold;

    @Value("${scoring.mapper.logdir}")
    private String scoringMapperLogDir;

    @Autowired
    private JdbcTemplate scoringJdbcTemplate;

    @Autowired
    private MapReduceCustomizationRegistry mapReduceCustomizationRegistry;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Autowired
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    @Autowired
    private DbCreds scoringCreds;

    private static final String JSON_SUFFIX = ".json";

    private static final String OUTPUT_TABLE_PREFIX = "Leads_";

    private static final String PID = "Pid";

    private static final Joiner commaJoiner = Joiner.on(", ").skipNulls();

    private static final Log log = LogFactory.getLog(ScoringStepYarnProcessorImpl.class);

    @Override
    @SuppressWarnings("incomplete-switch")
    public ApplicationId executeYarnStep(String deploymentExternalId, ScoringCommandStep currentStep,
            ScoringCommand scoringCommand) {
        ApplicationId appId = null;
        switch (currentStep) {
        case LOAD_DATA:
            appId = load(deploymentExternalId, scoringCommand);
            break;
        case SCORE_DATA:
            appId = score(deploymentExternalId, scoringCommand);
            break;
        case EXPORT_DATA:
            appId = export(deploymentExternalId, scoringCommand);
            break;
        }

        return appId;
    }

    private ApplicationId load(String customer, ScoringCommand scoringCommand) {
        String table = scoringCommand.getTableName();
        String targetDir = customerBaseDir + "/" + customer + "/scoring/" + table + "/data";
        metadataService.addPrimaryKeyColumn(scoringJdbcTemplate, table, PID);
        ApplicationId appId = sqoopSyncJobService.importData(table, targetDir, scoringCreds,
                LedpQueueAssigner.getMRQueueNameForSubmission(), customer, Arrays.asList(PID), "");
        return appId;
    }

    private ApplicationId score(String customer, ScoringCommand scoringCommand) {
        String table = scoringCommand.getTableName();
        Properties properties = new Properties();
        properties.setProperty(MapReduceProperty.CUSTOMER.name(), customer);
        properties.setProperty(MapReduceProperty.QUEUE.name(), LedpQueueAssigner.getMRQueueNameForSubmission());
        properties.setProperty(MapReduceProperty.INPUT.name(), customerBaseDir + "/" + customer + "/scoring/" + table
                + "/data");
        properties.setProperty(MapReduceProperty.OUTPUT.name(), customerBaseDir + "/" + customer + "/scoring/" + table
                + "/scores");
        properties.setProperty(ScoringProperty.LEAD_FILE_THRESHOLD.name(), leadFileThreshold);
        properties.setProperty(ScoringProperty.LEAD_INPUT_QUEUE_ID.name(), Long.toString(scoringCommand.getPid()));
        properties.setProperty(ScoringProperty.TENANT_ID.name(), scoringCommand.getId());
        properties.setProperty(ScoringProperty.LOG_DIR.name(), scoringMapperLogDir);

        String customerModelPath = customerBaseDir + "/" + customer + "/models";

        List<String> modelFilePaths = Collections.emptyList();
        try {
            modelFilePaths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, customerModelPath,
                    new HdfsFileFilter() {
                        @Override
                        public boolean accept(FileStatus fileStatus) {
                            if (fileStatus == null) {
                                return false;
                            }
                            Pattern p = Pattern.compile(".*model" + JSON_SUFFIX);
                            Matcher matcher = p.matcher(fileStatus.getPath().getName());
                            return matcher.matches();
                        }
                    });
        } catch (Exception e) {
            log.error("Customer " + customer + "'s scoring job failed due to: " + e.getMessage(), e);
        }
        if (CollectionUtils.isEmpty(modelFilePaths)) {
            throw new LedpException(LedpCode.LEDP_18023);
        }

        properties.setProperty(MapReduceProperty.CACHE_FILE_PATH.name(), commaJoiner.join(modelFilePaths));
        ApplicationId appId = jobService.submitMRJob("scoringJob", properties);

        return appId;
    }

    private ApplicationId export(String customer, ScoringCommand scoringCommand) {
        String queue = LedpQueueAssigner.getMRQueueNameForSubmission();
        String targetTable = createNewTable(customer, scoringCommand);
        // targetTable = "TestLeadsTable";
        DateTime dt = new DateTime(DateTimeZone.UTC);
        ScoringCommandResult result = new ScoringCommandResult(scoringCommand.getId(), ScoringCommandStatus.NEW,
                targetTable, 0, new Timestamp(dt.getMillis()));
        scoringCommandResultEntityMgr.create(result);
        ScoringCommandState state = scoringCommandStateEntityMgr.findLastStateByScoringCommand(scoringCommand);

        state.setLeadOutputQueuePid(result.getPid());
        scoringCommandStateEntityMgr.createOrUpdate(state);

        // scoringCreds.setDb("ScoringDB_buildmachine");
        // scoringCreds.setDBType("SQLServer");
        // scoringCreds.setHost("10.41.1.250");
        // scoringCreds.setPort(1433);
        String sourceDir = customerBaseDir + "/" + customer + "/scoring/" + scoringCommand.getTableName() + "/scores";
        ApplicationId appId = sqoopSyncJobService.exportData(targetTable, sourceDir, scoringCreds, queue, customer);

        return appId;
    }

    private String createNewTable(String customer, ScoringCommand scoringCommand) {
        // DataSource dataSource = new
        // DriverManagerDataSource("jdbc:sqlserver://10.41.1.250:1433;databaseName=ScoringDB_buildmachine",
        // "root", "welcome");
        // scoringJdbcTemplate.setDataSource(dataSource);
        String newTable = OUTPUT_TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
        metadataService.createNewEmptyTableFromExistingOne(scoringJdbcTemplate, newTable, targetRawTable);
        return newTable;
    }
}

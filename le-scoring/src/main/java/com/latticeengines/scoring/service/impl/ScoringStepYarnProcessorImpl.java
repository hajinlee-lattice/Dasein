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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
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
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scheduler.exposed.fairscheduler.LedpQueueAssigner;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.runtime.mapreduce.EventDataScoringJob;
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

    @Value("${scoring.datasource.host}")
    private String dbHost;

    @Value("${scoring.datasource.port}")
    private int dbPort;

    @Value("${scoring.datasource.dbname}")
    private String dbName;

    @Value("${scoring.datasource.user}")
    private String dbUser;

    @Value("${scoring.datasource.password.encrypted}")
    private String dbPassword;

    @Value("${scoring.datasource.type}")
    private String dbType;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Value("${scoring.output.table.sample}")
    private String targetRawTable;

    @Autowired
    private JdbcTemplate scoringJdbcTemplate;

    @Autowired
    private MapReduceCustomizationRegistry mapReduceCustomizationRegistry;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    private static final String JSON_SUFFIX = ".json";

    private static final String OUTPUT_TABLE_PREFIX = "Lead_";

    private static final Joiner commaJoiner = Joiner.on(", ").skipNulls();

    private static final Log log = LogFactory.getLog(ScoringStepYarnProcessorImpl.class);

    @VisibleForTesting
    void setDBConfig(String dbHost, int dbPort, String dbName, String dbUser, String dbPassword, String dbType) {
        this.dbHost = dbHost;
        this.dbPort = dbPort;
        this.dbName = dbName;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.dbType = dbType;
    }

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
//        case EXPORT_DATA:
//             appId = export(deploymentExternalId, scoringCommand);
//            break;
        }

        return appId;
    }

    private ApplicationId load(String customer, ScoringCommand scoringCommand){
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dbHost).port(dbPort).db(dbName).user(dbUser).password(dbPassword).dbType(dbType);
        DbCreds creds = new DbCreds(builder);
        String table = scoringCommand.getTableName();
        String targetDir = customerBaseDir + "/" + customer + "/scoring/data/" + table;

        ApplicationId appId = sqoopSyncJobService.importData(table, targetDir, creds,
                LedpQueueAssigner.getMRQueueNameForSubmission(), customer, Arrays.asList("Nutanix_EventTable_Clean"),
                "", 4);
        return appId;
    }

    private ApplicationId score(String customer, ScoringCommand scoringCommand) {
        String table = scoringCommand.getTableName();
        Properties properties = new Properties();
        properties.setProperty(MapReduceProperty.CUSTOMER.name(), customer);
        properties.setProperty(MapReduceProperty.QUEUE.name(), LedpQueueAssigner.getMRQueueNameForSubmission());

        properties.setProperty(MapReduceProperty.INPUT.name(), customerBaseDir + "/" + customer + "/scoring/data/" + table);
        properties.setProperty(MapReduceProperty.OUTPUT.name(), customerBaseDir + "/" + customer + "/scoring/scores/" + table);
        String customerModelPath = customerBaseDir + "/" + customer + "/models";

        List<String> modelFilePaths = Collections.emptyList();
        try {
            modelFilePaths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, customerModelPath, new HdfsFileFilter() {
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
            log.error(e.getMessage(), e);
        }
        if (CollectionUtils.isEmpty(modelFilePaths)) {
            throw new LedpException(LedpCode.LEDP_18023);
        }

        properties.setProperty(MapReduceProperty.CACHE_FILE_PATH.name(), commaJoiner.join(modelFilePaths));
        mapReduceCustomizationRegistry.register(new EventDataScoringJob(yarnConfiguration));
        ApplicationId appId = jobService.submitMRJob("scoringJob", properties);

        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dbHost).port(dbPort).db(dbName).user(dbUser).password(dbPassword).dbType(dbType);
        DbCreds creds = new DbCreds(builder);
        createNewTable(customer, creds);

        return appId;
    }

    private ApplicationId export(String customer, ScoringCommand scoringCommand) {
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dbHost).port(dbPort).db(dbName).user(dbUser).password(dbPassword).dbType(dbType);
        DbCreds creds = new DbCreds(builder);
        String queue = LedpQueueAssigner.getMRQueueNameForSubmission();
        String targetTable = createNewTable(customer, creds);

        ScoringCommandResult result = new ScoringCommandResult(scoringCommand.getId(), ScoringCommandStatus.NEW,
                targetTable, 0, new Timestamp(System.currentTimeMillis()));
        scoringCommandResultEntityMgr.create(result);

        String sourceDir = customerBaseDir + "/" + customer + "/scoring/scores/" + scoringCommand.getTableName();
        ApplicationId appId = sqoopSyncJobService.exportData(targetTable, sourceDir, creds, queue, customer, 4);

        return appId;
    }

    private String createNewTable(String customer, DbCreds creds) {
        String newTable = OUTPUT_TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "");
        String queue = LedpQueueAssigner.getMRQueueNameForSubmission();
        sqoopSyncJobService.eval(metadataService.createNewEmptyTableFromExistingOne(scoringJdbcTemplate, newTable, targetRawTable),
                queue, jobNameService.createJobName(customer, "create-table"), 1,
                metadataService.getJdbcConnectionUrl(creds));
        return newTable;
    }
}

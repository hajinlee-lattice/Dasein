package com.latticeengines.scoring.service.impl;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.dataplatform.exposed.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.dataplatform.exposed.service.JobNameService;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
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
import com.latticeengines.scoring.runtime.mapreduce.ScoringProperty;
import com.latticeengines.scoring.service.ScoringDaemonService;
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

    @Value("${scoring.mapper.max.input.split.size}")
    private String maxInputSplitSize;

    @Value("${scoring.mapper.min.input.split.size}")
    private String minInputSplitSize;

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
        metadataService.addPrimaryKeyColumn(scoringJdbcTemplate, table, PID);
        try {
            String scoringTableDir = customerBaseDir + "/" + tenant + "/scoring/" + table;
            if (HdfsUtils.fileExists(yarnConfiguration, scoringTableDir)) {
                HdfsUtils.rmdir(yarnConfiguration, scoringTableDir);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00004, new String[] { targetDir });
        }
        ApplicationId appId = sqoopSyncJobService.importData(table, targetDir, scoringCreds,
                LedpQueueAssigner.getScoringQueueNameForSubmission(), tenant, Arrays.asList(PID), "");
        return appId;
    }

    private ApplicationId score(ScoringCommand scoringCommand) {
        Properties properties = generateCustomizedProperties(scoringCommand);
        ApplicationId appId = jobService.submitMRJob("scoringJob", properties);
        return appId;
    }

    private ApplicationId export(ScoringCommand scoringCommand) {
        String queue = LedpQueueAssigner.getScoringQueueNameForSubmission();
        String targetTable = createNewTable(scoringCommand);
        String tenant = getTenant(scoringCommand);
        String sourceDir = customerBaseDir + "/" + tenant + "/scoring/" + scoringCommand.getTableName() + "/scores";
        ApplicationId appId = sqoopSyncJobService.exportData(targetTable, sourceDir, scoringCreds, queue, tenant);

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
        metadataService.createNewEmptyTableFromExistingOne(scoringJdbcTemplate, newTable, targetRawTable);
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
        properties.setProperty(ScoringProperty.LEAD_FILE_THRESHOLD.name(), leadFileThreshold);
        properties.setProperty(ScoringProperty.LEAD_INPUT_QUEUE_ID.name(), Long.toString(scoringCommand.getPid()));
        properties.setProperty(ScoringProperty.TENANT_ID.name(), tenant);
        properties.setProperty(ScoringProperty.LOG_DIR.name(), scoringMapperLogDir);

        List<String> modelUrls = findModelUrlsToLocalize(tenant, scoringCommand.getTableName());
        properties.setProperty(MapReduceProperty.CACHE_FILE_PATH.name(), commaJoiner.join(modelUrls));
        return properties;
    }

    private List<String> findAllModelPathsInHdfs(String tenant){
        String customerModelPath = customerBaseDir + "/" + tenant + "/models";
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
            log.error("Customer " + tenant + "'s scoring job failed due to: " + e.getMessage(), e);
        }
        if (CollectionUtils.isEmpty(modelFilePaths)) {
            throw new LedpException(LedpCode.LEDP_20008, new String[] { tenant });
        }
        return modelFilePaths;
    }

    private List<String> findModelUrlsToLocalize(String tenant, String tableName){
        List<String> modelGuids = metadataService.getDistinctColumnValues(scoringJdbcTemplate, tableName, ScoringDaemonService.MODEL_GUID);
        List<String> modelFilePaths = findAllModelPathsInHdfs(tenant);
        return findModelUrlsToLocalize(tenant, modelGuids, modelFilePaths);
    }

    @VisibleForTesting
    List<String> findModelUrlsToLocalize(String tenant, List<String> modelGuids, List<String> modelFilePaths){
        List<String> modelUrlsToLocalize = new ArrayList<>();
        label:
            for(String modelGuid : modelGuids){
                String uuid = UuidUtils.extractUuid(modelGuid);
                for (String path : modelFilePaths) {
                    if(uuid.equals(UuidUtils.parseUuid(path))){
                        try {
                            HdfsUtils.getCheckSum(yarnConfiguration, path);
                        }catch (IOException e) {
                            throw new LedpException(LedpCode.LEDP_20021, new String[]{path, tenant});
                        }
                        modelUrlsToLocalize.add(path + "#" + uuid);
                        continue label;
                    }
                }
                throw new LedpException(LedpCode.LEDP_18007, new String[]{modelGuid});
            }
        return modelUrlsToLocalize;
    }
}

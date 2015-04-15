package com.latticeengines.scoring.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
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
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.dataplatform.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.dataplatform.runtime.mapreduce.MapReduceProperty;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scheduler.exposed.fairscheduler.LedpQueueAssigner;
import com.latticeengines.scoring.runtime.mapreduce.EventDataScoringJob;
import com.latticeengines.scoring.service.ScoringStepYarnProcessor;

@Component("scoringStepYarnProcessor")
public class ScoringStepYarnProcessorImpl implements ScoringStepYarnProcessor {

    @Autowired
    private ModelingJobService modelingJobService;

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
    
    @Autowired
    private MapReduceCustomizationRegistry mapReduceCustomizationRegistry;

    private static final String JSON_SUFFIX = ".json";

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

        ApplicationId appId = modelingJobService.loadData(table, targetDir, creds,
                LedpQueueAssigner.getMRQueueNameForSubmission(), customer, Arrays.asList("Nutanix_EventTable_Clean"),
                new HashMap<String, String>(), 4);

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
        ApplicationId appId = modelingJobService.submitMRJob("scoringJob", properties);

        return appId;
    }

    private ApplicationId export(String deploymentExternalId, ScoringCommand scoringCommand) {
        // TODO Auto-generated method stub
        return null;
    }
}

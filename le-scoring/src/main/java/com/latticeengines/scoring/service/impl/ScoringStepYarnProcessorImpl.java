package com.latticeengines.scoring.service.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.dataplatform.runtime.mapreduce.MapReduceProperty;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scheduler.exposed.fairscheduler.LedpQueueAssigner;
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
//        case SCORE_DATA:
//            appId = score(deploymentExternalId, scoringCommand);
//            break;
//        case EXPORT_DATA:
//            // appIds = export(deploymentExternalId, modelCommand);
//            break;
        }

        return appId;
    }

    private ApplicationId load(String customer, ScoringCommand scoringCommand) {
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dbHost).port(dbPort).db(dbName).user(dbUser).password(dbPassword).dbType(dbType);
        DbCreds creds = new DbCreds(builder);
        String table = scoringCommand.getTableName();
        String targetDir = customerBaseDir + "/" + customer + "/scoring/data/" + table;
        ApplicationId appId = modelingJobService.loadData(table, targetDir, creds,
                LedpQueueAssigner.getMRQueueNameForSubmission(), customer, Arrays.asList("Nutanix_EventTable_Clean"),
                new HashMap<String, String>(), 4);
        // No LR for now.
        // ApplicationId pivotedAppId =
        // modelingService.loadData(generateLoadConfiguration(DataSetType.PIVOTED,
        // customer, commandParameters));
        // appIds.add(pivotedAppIds);

        return appId;
    }

    private ApplicationId score(String customer, ScoringCommand scoringCommand) {
        String table = scoringCommand.getTableName();
        Properties properties = new Properties();
        properties.setProperty(MapReduceProperty.QUEUE.name(), LedpQueueAssigner.getMRQueueNameForSubmission());
        properties.setProperty(MapReduceProperty.INPUT.name(), customerBaseDir + "/" + customer + "/scoring/data/" + table);
        properties.setProperty(MapReduceProperty.OUTPUT.name(), customerBaseDir + "/" + customer + "/scoring/result/" + table);
        ApplicationId appId = modelingJobService.submitMRJob("scoringJob", properties);
        return appId;
    }
}

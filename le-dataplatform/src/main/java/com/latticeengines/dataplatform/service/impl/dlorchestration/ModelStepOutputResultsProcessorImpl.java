package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.StringTokenUtils;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandOutput;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

@Component("modelStepOutputResultsProcessor")
public class ModelStepOutputResultsProcessorImpl implements ModelStepProcessor {

    public static final int SAMPLE_SIZE = 100;
    public static final String RANDOM_FOREST = "RandomForest";
    private static final String JSON_SUFFIX = ".json";
    private static final String CREATE_OUTPUT_TABLE_SQL = "(Id int NOT NULL,\n" + "    CommandId int NOT NULL,\n"
            + "    SampleSize int NOT NULL,\n" + "    Algorithm varchar(50) NOT NULL,\n"
            + "    JsonPath varchar(512) NULL,\n" + "    Timestamp datetime NULL\n" + ")\n" + "";

    private static final String HTTPFS_SUFFIX = "?op=OPEN&user.name=yarn";

    private static final String INSERT_OUTPUT_TABLE_SQL = "(Id, CommandId, SampleSize, Algorithm, JsonPath, Timestamp) values (?, ?, ?, ?, ?, ?)";

    @Value("${dataplatform.fs.web.defaultFS}")
    private String httpFsPrefix;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ModelingService modelingService;

    @Autowired
    private ModelCommandStateEntityMgr modelCommandStateEntityMgr;

    @Autowired
    private JdbcTemplate dlOrchestrationJdbcTemplate;

    @Autowired
    private ModelCommandLogService modelCommandLogService;

    @Override
    public void executeStep(ModelCommand modelCommand, ModelCommandParameters modelCommandParameters) {
        List<ModelCommandState> commandStates = modelCommandStateEntityMgr.findByModelCommandAndStep(modelCommand,
                ModelCommandStep.SUBMIT_MODELS);
        String appId = commandStates.get(0).getYarnApplicationId();
        JobStatus jobStatus = modelingService.getJobStatus(appId);
        String modelFilePath = "";
        try {
            for (String filePath : HdfsUtils.getFilesForDir(yarnConfiguration, jobStatus.getResultDirectory())) {
                if (filePath.endsWith(JSON_SUFFIX)) {
                    modelFilePath = filePath;
                    break;
                }
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_16002, e,
                    new String[] { String.valueOf(modelCommand.getPid()), appId });
        }
        // Provide link to diagnostics file
        String modelJsonFileHdfsPath = jobStatus.getResultDirectory() + "/" + StringTokenUtils.stripPath(modelFilePath);
        modelCommandLogService.log(modelCommand, "Model json file download link: " + httpFsPrefix + modelJsonFileHdfsPath
                + HTTPFS_SUFFIX);
        
        String modelCSVFileHdfsPath = jobStatus.getResultDirectory() + "/" + StringTokenUtils.stripPath(modelFilePath).replace(".json", ".csv");
        modelCommandLogService.log(modelCommand, "Top Predictors csv file download link: " + httpFsPrefix + modelCSVFileHdfsPath
                + HTTPFS_SUFFIX);
        
        String scoreFileHdfsPath = jobStatus.getResultDirectory() + "/scored.txt";
        modelCommandLogService.log(modelCommand, "Score file download link: " + httpFsPrefix + scoreFileHdfsPath
                + HTTPFS_SUFFIX);

        ModelCommandOutput output = new ModelCommandOutput(1, modelCommand.getPid().intValue(), SAMPLE_SIZE,
                RANDOM_FOREST, modelFilePath, new Date());
        dlOrchestrationJdbcTemplate.execute("drop table " + modelCommandParameters.getEventTable());
        dlOrchestrationJdbcTemplate.execute("create table " + modelCommandParameters.getEventTable() + " "
                + CREATE_OUTPUT_TABLE_SQL);
        dlOrchestrationJdbcTemplate.update("insert into " + modelCommandParameters.getEventTable() + " "
                + INSERT_OUTPUT_TABLE_SQL, output.getId(), output.getCommandId(), output.getSampleSize(),
                output.getAlgorithm(), output.getJsonPath(), output.getTimestamp());
    }

    @Override
    public Configuration getConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getCustomerBaseDir() {
        // TODO Auto-generated method stub
        return null;
    }

}

package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.StringTokenUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.camille.exposed.interfaces.data.DataInterfacePublisher;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandOutput;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("modelStepOutputResultsProcessor")
public class ModelStepOutputResultsProcessorImpl implements ModelStepProcessor {

    public static final int SAMPLE_SIZE = 100;
    public static final String RANDOM_FOREST = "RandomForest";
    private static final String JSON_SUFFIX = ".json";
    private static final String CSV_SUFFIX = ".csv";
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

        String modelFilePath = getModelFilePath(modelCommand, jobStatus, appId);

        publishModel(modelCommand, jobStatus, modelCommandParameters, modelFilePath, appId);
        publishModelArtifacts(modelCommand, jobStatus, modelCommandParameters, appId);
        publishLinks(modelCommand, jobStatus, modelFilePath);
        updateEventTable(modelCommand, modelCommandParameters, modelFilePath);
    }

    private String getModelFilePath(
            ModelCommand modelCommand,
            JobStatus jobStatus,
            String appId) throws LedpException {
        try {

            String hdfsResultDirectory = jobStatus.getResultDirectory();

            //Assume one non-diagnostic json file in hdfsResultDirectory!
            List<String> jsonFiles = HdfsUtils.getFilesForDir(
                    yarnConfiguration,
                    hdfsResultDirectory,
                    new HdfsFilenameFilter() {
                        @Override
                        public boolean accept(String filename) {
                            if (filename.equals("diagnostics" + JSON_SUFFIX)) { return false; }
                            Pattern p = Pattern.compile(".*" + JSON_SUFFIX);
                            Matcher matcher = p.matcher(filename.toString());
                            return matcher.matches();
                        }});

            if (jsonFiles.size() == 1) {
                return jsonFiles.get(0);
            } else if (jsonFiles.size() == 0) {
                throw new Exception("Model file does not exist.");
            } else {
                throw new Exception("Too many model files exist.");
            }

        }
        catch (Exception e) {
          throw new LedpException(LedpCode.LEDP_16002, e,
                  new String[] { String.valueOf(modelCommand.getPid()), appId });
        }
    }

    private String getUniqueModelName(
            JobStatus jobStatus,
            ModelCommandParameters modelCommandParameters) {
        //This is apparently a BARD restriction.
        int modelIdLengthLimit = 45;

        String modelName = modelCommandParameters.getModelName();
        
        String modelId = jobStatus.getResultDirectory().split("/")[7] + "-" + modelName;
        if (modelId.length() > modelIdLengthLimit) {
            modelId = modelId.substring(0, modelIdLengthLimit);
        }
        
        return modelId;
    }
    
    private int getModelVersion()
    {
        // TODO This should be set to the actual model version once that's supported.
    	return 1;
    }
    
    private void publishModel(
            ModelCommand modelCommand,
            JobStatus jobStatus,
            ModelCommandParameters modelCommandParameters,
            String modelFilePath,
            String appId) throws LedpException {
        try {

            String hdfsResultDirectory = jobStatus.getResultDirectory();

            String[] tokens = hdfsResultDirectory.split("/");

            if (tokens.length > 7) { //Chances are, this is good.
                String consumer = "BARD";

                StringBuilder hdfsCustomersDirectory = new StringBuilder();
                for (int i = 0; i < 4; i++) {
                	hdfsCustomersDirectory.append(tokens[i]).append("/");
                }
                
                String modelId = getUniqueModelName(jobStatus, modelCommandParameters);

                StringBuilder hdfsConsumerDirectory = new StringBuilder();
                hdfsConsumerDirectory.append(hdfsCustomersDirectory).append(tokens[4]).append("/").append(consumer).append("/").append(modelId);
                String hdfsConsumerFile = hdfsConsumerDirectory + "/" + getModelVersion() + ".json";

                if (HdfsUtils.fileExists(yarnConfiguration, hdfsConsumerFile)) {
                    HdfsUtils.rmdir(yarnConfiguration, hdfsConsumerFile);
                }

                String modelContent = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelFilePath);
                HdfsUtils.writeToFile(yarnConfiguration, hdfsConsumerFile, modelContent);
                
                // Publish the PMML model and associated artifacts.
                String deploymentExternalId = modelCommand.getDeploymentExternalId();
                CustomerSpace space = new CustomerSpace(deploymentExternalId);
                
                StringBuilder artifactPath = new StringBuilder();
                artifactPath.append(hdfsCustomersDirectory).append(space).append("/models/").append(modelId).append("/").append(getModelVersion()).append("/");
                
                String pmmlContent = HdfsUtils.getHdfsFileContents(yarnConfiguration, hdfsResultDirectory + "/rfpmml.xml");
                HdfsUtils.writeToFile(yarnConfiguration, artifactPath + "ModelPmml.xml", pmmlContent);
                
                String dataContent = HdfsUtils.getHdfsFileContents(yarnConfiguration, hdfsResultDirectory + "/enhancements/DataComposition.json");
                HdfsUtils.writeToFile(yarnConfiguration, artifactPath + "DataComposition.json", dataContent);
                
                String scoreContent = HdfsUtils.getHdfsFileContents(yarnConfiguration, hdfsResultDirectory + "/enhancements/ScoreDerivation.json");
                HdfsUtils.writeToFile(yarnConfiguration, artifactPath + "ScoreDerivation.json", scoreContent);
            } else {               
                throw new Exception("Unexpected result directory: " + hdfsResultDirectory);
            }
        }
        catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_16010, e,
                  new String[] { String.valueOf(modelCommand.getPid()), appId });
        }
    }

    private void publishModelArtifacts(
            ModelCommand modelCommand,
            JobStatus jobStatus,
            ModelCommandParameters modelCommandParameters,
            String appId) throws LedpException {
        try {

            String hdfsPath = jobStatus.getResultDirectory() + "/enhancements/modelsummary.json";

            String content = HdfsUtils.getHdfsFileContents(yarnConfiguration, hdfsPath);

            String interfaceName = "ModelSummary";
            String deploymentExternalId = modelCommand.getDeploymentExternalId();
            CustomerSpace space = new CustomerSpace(deploymentExternalId);
            DataInterfacePublisher pub = new DataInterfacePublisher(interfaceName, space);

            Path relativePath = new Path("/ModelSummary");
            Document pubDoc = new Document(content);
            pub.publish(relativePath, pubDoc);

            // Publish model artifacts into ZooKeeper.            
            DataInterfacePublisher modelPublisher = new DataInterfacePublisher("ModelArtifact", space);
            String modelName = getUniqueModelName(jobStatus, modelCommandParameters);
            String basePath = "/Models/" + modelName + "/" + getModelVersion() + "/";

            String dataComposition = HdfsUtils.getHdfsFileContents(yarnConfiguration,
            		jobStatus.getResultDirectory() + "/enhancements/DataComposition.json");
            modelPublisher.publish(new Path(basePath + "DataComposition.json"), new Document(dataComposition));

            String scoreDerivation = HdfsUtils.getHdfsFileContents(yarnConfiguration,
            		jobStatus.getResultDirectory() + "/enhancements/ScoreDerivation.json");
            modelPublisher.publish(new Path(basePath + "ScoreDerivation.json"), new Document(scoreDerivation));
        }
        catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_16011, e,
                  new String[] { String.valueOf(modelCommand.getPid()), appId });
        }
    }

    private void publishLinks(
            ModelCommand modelCommand,
            JobStatus jobStatus,
            String modelFilePath) {

        // Provide link to result files
        String modelJsonFileHdfsPath = jobStatus.getResultDirectory() + "/" + StringTokenUtils.stripPath(modelFilePath);
        modelCommandLogService.log(modelCommand, "Model json file download link: " + httpFsPrefix + modelJsonFileHdfsPath
                + HTTPFS_SUFFIX);
        
        String modelCSVFileHdfsPath = jobStatus.getResultDirectory() + "/" + StringTokenUtils.stripPath(modelFilePath).replace(JSON_SUFFIX, CSV_SUFFIX);
        modelCommandLogService.log(modelCommand, "Top Predictors csv file download link: " + httpFsPrefix + modelCSVFileHdfsPath
                + HTTPFS_SUFFIX);
        
        String scoreFileHdfsPath = jobStatus.getResultDirectory() + "/scored.txt";
        modelCommandLogService.log(modelCommand, "Score file download link: " + httpFsPrefix + scoreFileHdfsPath
                + HTTPFS_SUFFIX);

        String readOutSampleFileHdfsPath = jobStatus.getResultDirectory() + "/readoutsample.csv";
        modelCommandLogService.log(modelCommand, "ReadOutSample file download link: " + httpFsPrefix + readOutSampleFileHdfsPath
                + HTTPFS_SUFFIX);
    }
    

    private void updateEventTable(
            ModelCommand modelCommand,
            ModelCommandParameters modelCommandParameters,
            String modelFilePath) {

        ModelCommandOutput output = new ModelCommandOutput(1, modelCommand.getPid().intValue(), SAMPLE_SIZE,
                RANDOM_FOREST, modelFilePath, new Date());
        dlOrchestrationJdbcTemplate.execute("drop table " + modelCommandParameters.getEventTable());
        dlOrchestrationJdbcTemplate.execute("create table " + modelCommandParameters.getEventTable() + " "
                + CREATE_OUTPUT_TABLE_SQL);
        dlOrchestrationJdbcTemplate.update("insert into " + modelCommandParameters.getEventTable() + " "
                + INSERT_OUTPUT_TABLE_SQL, output.getId(), output.getCommandId(), output.getSampleSize(),
                output.getAlgorithm(), output.getJsonPath(), output.getTimestamp());
    }
}

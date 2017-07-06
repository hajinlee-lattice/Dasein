package com.latticeengines.pls.service.impl;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.service.ModelReplaceService;
import com.latticeengines.pls.util.ModelingHdfsUtils;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("modelReplaceService")
public class ModelReplaceServiceImpl implements ModelReplaceService {

    private static Logger log = Logger.getLogger(ModelReplaceServiceImpl.class);

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${pls.modelingservice.basedir}")
    private String customerBase;

    /**
     * 
     * @param sourceTenantId
     *            source tenant has the model we want to use to replace
     * @param sourceModelId
     *            source model id used to find the model to replace with
     * @param targetTenantId
     *            target tenant has the model we want to replace
     * @param targetModelId
     *            target model id used to find the model to replace
     * @return
     */
    @Override
    public boolean replaceModel(String sourceTenantId, String sourceModelId, String targetTenantId,
            String targetModelId) {

        try {
            ModelSummary sourceModelSummary = modelSummaryEntityMgr.getByModelId(sourceModelId);
            ModelSummary targetModelSummary = modelSummaryEntityMgr.getByModelId(targetModelId);

            processHdfsData(sourceTenantId, targetTenantId, sourceModelSummary, targetModelSummary);
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException(e);
        }
        return true;
    }

    void processHdfsData(String sourceTenantId, String targetTenantId, ModelSummary sourceModelSummary,
            ModelSummary targetModelSummary) throws IOException {
        String sourceCustomerRoot = customerBase + sourceTenantId;
        String targetCustomerRoot = customerBase + targetTenantId;

        String sourceEventTableName = sourceModelSummary.getEventTableName();
        String targetEventTableName = targetModelSummary.getEventTableName();
        copyDataComposition(sourceCustomerRoot, targetCustomerRoot, sourceEventTableName, targetEventTableName);

        String sourceModelRoot = sourceCustomerRoot + "/models/" + sourceEventTableName;
        String targetModelRoot = targetCustomerRoot + "/models/" + targetEventTableName;

        String sourceModelSummaryPath = ModelingHdfsUtils.findModelSummaryPath(yarnConfiguration, sourceModelRoot);
        String sourceModelDirPath = new Path(sourceModelSummaryPath).getParent().getParent().toString();
        String sourceModelLocalRoot = new Path(sourceModelDirPath).getName();

        FileUtils.deleteDirectory(new File(sourceModelLocalRoot));
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, sourceModelDirPath, ".");

        String sourceModelSummaryLocalPath = sourceModelLocalRoot + "/enhancements/modelsummary.json";
        JsonNode newModelSummary = constructNewModelSummary(sourceModelSummaryLocalPath, targetModelSummary);
        String sourceModelFileName = getModelFileName(sourceModelLocalRoot, true);
        JsonNode newModel = ModelingHdfsUtils.constructNewModel(sourceModelLocalRoot + "/" + sourceModelFileName,
                targetModelSummary.getId());

        FileUtils.deleteQuietly(new File(sourceModelLocalRoot + "/enhancements/.modelsummary.json.crc"));
        FileUtils.write(new File(sourceModelSummaryLocalPath), newModelSummary.toString(), "UTF-8", false);

        String targetModelSummaryPath = ModelingHdfsUtils.findModelSummaryPath(yarnConfiguration, targetModelRoot);
        String targetModelDirPath = new Path(targetModelSummaryPath).getParent().getParent().toString();
        String targetModelFileName = getModelFileName(targetModelDirPath, false);
        FileUtils.deleteQuietly(new File(sourceModelLocalRoot + "/." + sourceModelFileName));
        FileUtils.deleteQuietly(new File(sourceModelLocalRoot + "/." + sourceModelFileName + ".crc"));
        FileUtils.write(new File(sourceModelLocalRoot + "/" + targetModelFileName), newModel.toString(), "UTF-8",
                false);
        copyModelingArtifacts(sourceModelLocalRoot, targetModelDirPath, targetModelFileName);

    }

    private void copyModelingArtifacts(String sourceModelLocalRoot, String targetModelDirPath,
            String targetModelFileName) throws IOException {
        for (String fileName : new File(sourceModelLocalRoot + "/enhancements").list()) {
            if (!fileName.endsWith(".crc")) {
                backupFileAndCopy(sourceModelLocalRoot + "/enhancements", targetModelDirPath + "/enhancements",
                        fileName);
            }
        }
        backupFileAndCopy(sourceModelLocalRoot, targetModelDirPath, targetModelFileName);
        backupFileAndCopy(sourceModelLocalRoot, targetModelDirPath, "rfpmml.xml");
        backupFileAndCopy(sourceModelLocalRoot, targetModelDirPath, "rf_model.txt");
    }

    void backupFileAndCopy(String sourceLocalPath, String targetDirPath, String fileName) throws IOException {
        if (HdfsUtils.fileExists(yarnConfiguration, targetDirPath + "/" + fileName)) {
            if (!HdfsUtils.fileExists(yarnConfiguration, targetDirPath + "/" + fileName + ".bak")) {
                HdfsUtils.moveFile(yarnConfiguration, targetDirPath + "/" + fileName,
                        targetDirPath + "/" + fileName + ".bak");
            } else {
                HdfsUtils.rmdir(yarnConfiguration, targetDirPath + "/" + fileName);
            }
        }
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, sourceLocalPath + "/" + fileName, targetDirPath);
    }

    JsonNode constructNewModelSummary(String modelSummaryLocalPath, ModelSummary targetModelSummary)
            throws IOException {
        String contents = FileUtils.readFileToString(new File(modelSummaryLocalPath), "UTF-8");
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode json = objectMapper.readTree(contents);

        ObjectNode detail = (ObjectNode) json.get("ModelDetails");
        detail.put("ModelID", targetModelSummary.getId());
        detail.put("ConstructionTime", targetModelSummary.getConstructionTime());
        detail.put("LookupID", targetModelSummary.getLookupId());
        detail.put("DisplayName", targetModelSummary.getDisplayName());
        detail.put("RocScore", targetModelSummary.getRocScore());
        detail.put("Name", targetModelSummary.getName());

        ObjectNode provenance = (ObjectNode) json.get("EventTableProvenance");
        provenance.put("TrainingTableName", targetModelSummary.getTrainingTableName());
        provenance.put("EventTableName", targetModelSummary.getEventTableName());
        return json;

    }

    private void copyDataComposition(String sourceCustomerRoot, String targetCustomerRoot, String sourceEventTableName,
            String targetEventTableName) throws IOException {
        String sourceStandardDataCompositionPath = ModelingHdfsUtils.getStandardDataComposition(yarnConfiguration,
                sourceCustomerRoot + "/data/", sourceEventTableName);
        String targetStandardDataCompositonPath = ModelingHdfsUtils.getStandardDataComposition(yarnConfiguration,
                targetCustomerRoot + "/data/", targetEventTableName);

        if (!HdfsUtils.fileExists(yarnConfiguration, targetStandardDataCompositonPath + ".bak")) {
            HdfsUtils.moveFile(yarnConfiguration, targetStandardDataCompositonPath,
                    targetStandardDataCompositonPath + ".bak");
        } else {
            HdfsUtils.rmdir(yarnConfiguration, targetStandardDataCompositonPath);
        }
        HdfsUtils.copyFiles(yarnConfiguration, sourceStandardDataCompositionPath, targetStandardDataCompositonPath);
    }

    String getModelFileName(String path, boolean isLocal) throws IllegalArgumentException, IOException {
        Configuration config = yarnConfiguration;
        if (isLocal) {
            config = new Configuration();
            config.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        }
        return ModelingHdfsUtils.getModelFileName(config, path);
    }

    @Override
    public boolean replaceModel(String sourceModelId, String targetTenantId, String targetModelId) {
        CustomerSpace space = MultiTenantContext.getCustomerSpace();
        return replaceModel(space.toString(), sourceModelId, targetTenantId, targetModelId);
    }
}

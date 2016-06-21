package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.pls.service.ModelCopyService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("modelCopyService")
public class ModelCopyServiceImpl implements ModelCopyService {

    private static Logger log = Logger.getLogger(ModelCopyServiceImpl.class);

    @Autowired
    private TenantService tenantService;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${pls.modelingservice.basedir}")
    private String customerBase;

    @Override
    public Boolean copyModel(String sourceTenantId, String targetTenantId, String modelId) {
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        String trainingTableName = modelSummary.getTrainingTableName();
        String eventTableName = modelSummary.getEventTableName();

        Table cpTrainingTable = metadataProxy.copyTable(sourceTenantId, trainingTableName, targetTenantId);
        Table cpEventTable = metadataProxy.copyTable(sourceTenantId, eventTableName, targetTenantId);

        try {
            processHdfsData(sourceTenantId, targetTenantId, modelId, eventTableName, cpTrainingTable.getName(),
                    cpEventTable.getName());
        } catch (IOException e) {
            log.error(e);
            throw new LedpException(LedpCode.LEDP_18111, new String[]{modelSummary.getName(), sourceTenantId, targetTenantId});
        }
        return true;
    }

    @VisibleForTesting
    void processHdfsData(String sourceTenantId, String targetTenantId, String modelId, String eventTableName,
            String cpTrainingTableName, String cpEventTableName) throws IOException {
        String sourceCustomerRoot = customerBase + sourceTenantId;
        String targetCustomerRoot = customerBase + targetTenantId;

        copyModelingDataDirectory(sourceCustomerRoot, targetCustomerRoot, eventTableName, cpEventTableName);

        String sourceModelRoot = sourceCustomerRoot + "/models/" + eventTableName + "/"
                + UuidUtils.extractUuid(modelId);
        String modelSummaryPath = findModelSummaryPath(sourceModelRoot);
        String modelPath = getModelPath(modelSummaryPath);
        String uuid = UUID.randomUUID().toString();

        JsonNode newModel = constructNewModel(modelPath, uuid);
        JsonNode newModelSummary = constructNewModelSummary(modelSummaryPath,
                targetTenantId, cpTrainingTableName, cpEventTableName, uuid);

        copyModelingModelsDirectory(modelSummaryPath, targetCustomerRoot, cpEventTableName,
                uuid);
 
        String newModelSummaryPath = getModelSummaryPathFromTargetDir(modelSummaryPath, targetTenantId, cpEventTableName, uuid);
        HdfsUtils.rmdir(yarnConfiguration, newModelSummaryPath);
        String newModelPath = getModelPath(newModelSummaryPath);
        HdfsUtils.rmdir(yarnConfiguration, newModelPath);

        HdfsUtils.writeToFile(yarnConfiguration, newModelSummaryPath, newModelSummary.toString());
        HdfsUtils.writeToFile(yarnConfiguration, newModelPath, newModel.toString());
    }

    private JsonNode constructNewModel(String modelPath, String uuid) throws IOException {
        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelPath);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode json = objectMapper.readTree(contents);

        ObjectNode summary = (ObjectNode) json.get("Summary");
        summary.put("ModelID", "ms__" + uuid + "-PLSModel");
        return json;
    }

    String getModelPath(String modelSummaryPath) throws IllegalArgumentException, IOException{
        List<String> paths = HdfsUtils.getFilesForDir(yarnConfiguration, new Path(modelSummaryPath).getParent().getParent().toString(), ".*.model.json");
        if (paths.size() == 0) {
            throw new LedpException(LedpCode.LEDP_00002);
        }
        return paths.get(0);
    }

    JsonNode constructNewModelSummary(String modelSummaryPath, String targetTenantId, String cpTrainingTableName, String cpEventTableName, String uuid) throws IOException {

        String contents = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelSummaryPath);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode json = objectMapper.readTree(contents);

        ObjectNode detail = (ObjectNode) json.get("ModelDetails");

        detail.put("ModelID", "ms__" + uuid + "-PLSModel");
        detail.put("ConstructionTime", new DateTime().getMillis() / 1000);
        detail.put("LookupID", String.format("%s|%s|%s", targetTenantId, cpEventTableName, uuid));

        ObjectNode provenance = (ObjectNode) json.get("EventTableProvenance");
        provenance.put("TrainingTableName", cpTrainingTableName);
        provenance.put("EventTableName", cpEventTableName);
        return json;

    }

    private String findModelSummaryPath(String dir) throws IOException {
        List<String> paths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, dir, new HdfsUtils.HdfsFileFilter() {

            @Override
            public boolean accept(FileStatus file) {
                if (file == null) {
                    return false;
                }
                String name = file.getPath().getName().toString();
                return name.equals("modelsummary.json");
            }

        });
        if (paths.size() == 0) {
            throw new LedpException(LedpCode.LEDP_00002);
        }
        return paths.get(0);
    }

    void copyModelingDataDirectory(String sourceCustomerRoot, String targetCustomerRoot, String eventTableName,
            String cpEventTableName) throws IOException {
        String sourceDataRoot = sourceCustomerRoot + "/data/" + eventTableName;
        String targetDataRoot = targetCustomerRoot + "/data/";
        HdfsUtils.mkdir(yarnConfiguration, targetDataRoot);
        HdfsUtils.copyFiles(yarnConfiguration, sourceDataRoot, targetDataRoot);
        HdfsUtils.moveFile(yarnConfiguration, targetDataRoot + eventTableName, targetDataRoot + cpEventTableName);
    }

    void copyModelingModelsDirectory(String modelSummaryPath, String targetCustomerRoot,
            String cpEventTableName, String uuid) throws IOException {
        String sourceModelRoot = new Path(modelSummaryPath).getParent().getParent().toString();
        String targetModelRoot = targetCustomerRoot + "/models/" + cpEventTableName + "/" + uuid;
        HdfsUtils.mkdir(yarnConfiguration, targetModelRoot);
        HdfsUtils.copyFiles(yarnConfiguration, sourceModelRoot, targetModelRoot);
    }

    String getModelSummaryPathFromTargetDir(String modelSummaryPath, String targetTenantId, String cpEventTableName, String uuid) throws IOException {
        String[] tokens = modelSummaryPath.split("/");
        tokens[6] = targetTenantId;
        tokens[8] = cpEventTableName;
        tokens[9] = uuid;
        return StringUtils.join(tokens, "/");
    }

    @Override
    public Boolean copyModel(String targetTenantId, String modelId) {
        CustomerSpace space = MultiTenantContext.getCustomerSpace();
        return copyModel(space.toString(), targetTenantId, modelId);
    }
}

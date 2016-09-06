package com.latticeengines.pls.service.impl;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.SourceFileEntityMgr;
import com.latticeengines.pls.service.ModelCopyService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.util.ModelingHdfsUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
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

    @Autowired
    private SourceFileEntityMgr sourceFileEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Value("${pls.modelingservice.basedir}")
    private String customerBase;

    @Override
    public boolean copyModel(String sourceTenantId, String targetTenantId, String modelId) {
        ModelSummary modelSummary = modelSummaryService.getModelSummaryByModelId(modelId);
        if (modelSummary == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        String trainingTableName = modelSummary.getTrainingTableName();
        String eventTableName = modelSummary.getEventTableName();

        Table cpTrainingTable = metadataProxy.copyTable(sourceTenantId, trainingTableName, targetTenantId);
        Table cpEventTable = metadataProxy.copyTable(sourceTenantId, eventTableName, targetTenantId);

        Tenant targetTenant = tenantEntityMgr.findByTenantId(targetTenantId);
        SourceFile sourceFile = sourceFileEntityMgr.findByTableName(trainingTableName);
        if (sourceFile != null) {
            sourceFile.setPid(null);
            sourceFile.setTableName(cpTrainingTable.getName());
            sourceFile.setTenant(targetTenant);
            sourceFile.setName("file_" + cpTrainingTable.getName());
            sourceFileEntityMgr.create(sourceFile);
        }

        try {
            processHdfsData(sourceTenantId, targetTenantId, modelId, eventTableName, cpTrainingTable.getName(),
                    cpEventTable.getName(), modelSummary.getDisplayName());
        } catch (IOException e) {
            log.error(e);
            throw new LedpException(LedpCode.LEDP_18111, new String[] { modelSummary.getName(), sourceTenantId,
                    targetTenantId });
        }
        return true;
    }

    @VisibleForTesting
    void processHdfsData(String sourceTenantId, String targetTenantId, String modelId, String eventTableName,
            String cpTrainingTableName, String cpEventTableName, String modelDisplayName) throws IOException {
        String sourceCustomerRoot = customerBase + sourceTenantId;
        String targetCustomerRoot = customerBase + targetTenantId;

        String sourceModelRoot = sourceCustomerRoot + "/models/" + eventTableName + "/"
                + UuidUtils.extractUuid(modelId);
        String sourceModelSummaryPath = ModelingHdfsUtils.findModelSummaryPath(yarnConfiguration, sourceModelRoot);
        String uuid = UUID.randomUUID().toString();
        String sourceModelDirPath = new Path(sourceModelSummaryPath).getParent().getParent().toString();
        String sourceModelLocalRoot = new Path(sourceModelDirPath).getName();
        String modelSummaryLocalPath = sourceModelLocalRoot + "/enhancements/modelsummary.json";

        copyModelingDataDirectory(sourceCustomerRoot, targetCustomerRoot, eventTableName, cpEventTableName);
        FileUtils.deleteDirectory(new File(sourceModelLocalRoot));
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, sourceModelDirPath, ".");

        String modelFileName = getModelFileName(sourceModelLocalRoot);
        JsonNode newModelSummary = constructNewModelSummary(modelSummaryLocalPath, targetTenantId, cpTrainingTableName,
                cpEventTableName, uuid, modelDisplayName);
        JsonNode newModel = ModelingHdfsUtils.constructNewModel(sourceModelLocalRoot + "/" + modelFileName, "ms__"
                + uuid + "-PLSModel");

        FileUtils.deleteQuietly(new File(sourceModelLocalRoot + "/enhancements/.modelsummary.json.crc"));
        FileUtils.write(new File(modelSummaryLocalPath), newModelSummary.toString(), "UTF-8", false);

        String targetModelRoot = targetCustomerRoot + "/models/" + cpEventTableName + "/" + uuid;

        FileUtils.deleteQuietly(new File(sourceModelLocalRoot + "/." + modelFileName + ".crc"));
        FileUtils.write(new File(sourceModelLocalRoot + "/" + modelFileName), newModel.toString(), "UTF-8", false);

        HdfsUtils.mkdir(yarnConfiguration, targetModelRoot);
        HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, sourceModelLocalRoot, targetModelRoot);
        FileUtils.deleteDirectory(new File(sourceModelLocalRoot));
    }

    String getModelFileName(String sourceModelLocalRoot) throws IllegalArgumentException, IOException {
        Configuration localFileSystemConfig = new Configuration();
        localFileSystemConfig.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        return ModelingHdfsUtils.getModelFileName(localFileSystemConfig, sourceModelLocalRoot);
    }

    JsonNode constructNewModelSummary(String modelSummaryLocalPath, String targetTenantId, String cpTrainingTableName,
            String cpEventTableName, String uuid, String modelDisplayName) throws IOException {
        String contents = FileUtils.readFileToString(new File(modelSummaryLocalPath), "UTF-8");
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode json = objectMapper.readTree(contents);

        ObjectNode detail = (ObjectNode) json.get("ModelDetails");
        detail.put("ModelID", "ms__" + uuid + "-PLSModel");
        detail.put("ConstructionTime", new DateTime().getMillis() / 1000);
        detail.put("LookupID", String.format("%s|%s|%s", targetTenantId, cpEventTableName, uuid));
        detail.put("DisplayName", modelDisplayName);

        ObjectNode provenance = (ObjectNode) json.get("EventTableProvenance");
        provenance.put("TrainingTableName", cpTrainingTableName);
        provenance.put("EventTableName", cpEventTableName);
        return json;

    }

    void copyModelingDataDirectory(String sourceCustomerRoot, String targetCustomerRoot, String eventTableName,
            String cpEventTableName) throws IOException {
        String sourceDataRoot = sourceCustomerRoot + "/data/" + eventTableName;
        String targetDataRoot = targetCustomerRoot + "/data/" + cpEventTableName;
        HdfsUtils.copyFiles(yarnConfiguration, sourceDataRoot, targetDataRoot);

        String sourceStandardDataCompositionPath = ModelingHdfsUtils.getStandardDataComposition(yarnConfiguration,
                sourceCustomerRoot + "/data/", eventTableName);
        String targetStandardDataCompositionPath = sourceStandardDataCompositionPath.replace(sourceCustomerRoot,
                targetCustomerRoot).replace(eventTableName, cpEventTableName);
        HdfsUtils.copyFiles(yarnConfiguration, new Path(sourceStandardDataCompositionPath).getParent().toString(),
                new Path(targetStandardDataCompositionPath).getParent().toString());
    }

    @Override
    public boolean copyModel(String targetTenantId, String modelId) {
        CustomerSpace space = MultiTenantContext.getCustomerSpace();
        return copyModel(space.toString(), targetTenantId, modelId);
    }
}

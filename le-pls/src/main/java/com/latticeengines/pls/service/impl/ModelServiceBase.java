package com.latticeengines.pls.service.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.ModelService;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.util.ModelingHdfsUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Component
public abstract class ModelServiceBase implements ModelService {

    private static Map<ModelType, ModelService> registry = new HashMap<>();

    protected ModelServiceBase(ModelType modelType) {
        registry.put(modelType, this);
    }

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected MetadataProxy metadataProxy;

    @Autowired
    protected ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    protected TenantEntityMgr tenantEntityMgr;

    @Value("${pls.modelingservice.basedir}")
    protected String customerBaseDir;

    public static ModelService getModelService(String modelTypeStr) {
        if (modelTypeStr == null) {
            return registry.get(ModelType.PYTHONMODEL);
        }
        ModelType modelType = ModelType.getByModelType(modelTypeStr);

        if (modelType == null) {
            throw new NullPointerException("Unknown model type " + modelTypeStr);
        }

        return registry.get(modelType);
    }

    @Override
    public List<String> getRequiredColumnDisplayNames(String modelId) {
        List<String> requiredColumnDisplayNames = new ArrayList<String>();
        List<Attribute> requiredColumns = getRequiredColumns(modelId);
        for (Attribute column : requiredColumns) {
            requiredColumnDisplayNames.add(column.getDisplayName());
        }
        return requiredColumnDisplayNames;
    }

    @VisibleForTesting
    void copyHdfsData(String sourceTenantId, String targetTenantId, String eventTableName, String cpTrainingTableName,
            String cpEventTableName, ModelSummary modelSummary) throws IOException {
        String sourceCustomerRoot = customerBaseDir + sourceTenantId;
        String targetCustomerRoot = customerBaseDir + targetTenantId;

        ModelingHdfsUtils.copyModelingDataDirectory(sourceCustomerRoot, targetCustomerRoot, eventTableName,
                cpEventTableName, yarnConfiguration);

        String sourceModelRoot = sourceCustomerRoot + "/models/" + eventTableName + "/"
                + UuidUtils.extractUuid(modelSummary.getId());
        String sourceModelSummaryPath = ModelingHdfsUtils.findModelSummaryPath(yarnConfiguration, sourceModelRoot);
        String uuid = UUID.randomUUID().toString();
        String sourceModelDirPath = new Path(sourceModelSummaryPath).getParent().getParent().toString();
        String sourceModelLocalRoot = new Path(sourceModelDirPath).getName();
        String modelSummaryLocalPath = sourceModelLocalRoot + "/enhancements/modelsummary.json";

        FileUtils.deleteDirectory(new File(sourceModelLocalRoot));
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, sourceModelDirPath, ".");

//        if (StringUtils.isNotEmpty(modelSummary.getModuleName())){
//            
//        }else{
        String newPivotFilePath = ModelingHdfsUtils.copyPivotMappingFile(yarnConfiguration, modelSummary, targetTenantId);
        //}
        String contents = FileUtils.readFileToString(new File(modelSummaryLocalPath), "UTF-8");
        JsonNode newModelSummary = ModelingHdfsUtils.constructNewModelSummary(contents, targetTenantId,
                cpTrainingTableName, cpEventTableName, uuid, modelSummary.getDisplayName(), newPivotFilePath);

        String modelFileName = ModelingHdfsUtils.getModelFileNameFromLocalDir(sourceModelLocalRoot);
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
}

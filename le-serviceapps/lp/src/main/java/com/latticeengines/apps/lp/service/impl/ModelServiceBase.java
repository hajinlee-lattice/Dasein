package com.latticeengines.apps.lp.service.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.lp.service.ModelSummaryService;
import com.latticeengines.apps.lp.service.SourceFileService;
import com.latticeengines.apps.lp.util.ModelingHdfsUtils;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Module;
import com.latticeengines.domain.exposed.pls.ModelService;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component
public abstract class ModelServiceBase implements ModelService {

    private static final Logger log = LoggerFactory.getLogger(ModelServiceBase.class);
    private static Map<ModelType, ModelService> registry = new HashMap<>();

    protected ModelServiceBase(ModelType modelType) {
        registry.put(modelType, this);
    }

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected MetadataProxy metadataProxy;

    @Autowired
    protected TenantEntityMgr tenantEntityMgr;

    @Autowired
    protected SourceFileService sourceFileService;

    @Autowired
    protected ModelSummaryService modelSummaryService;

    @Value("${pls.modelingservice.basedir}")
    protected String customerBaseDir;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

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
    String copyHdfsData(String sourceTenantId, String targetTenantId, String eventTableName, String cpTrainingTableName,
            String cpEventTableName, ModelSummary modelSummary) throws IOException {
        String sourceCustomerRoot = customerBaseDir + sourceTenantId;
        String targetCustomerRoot = customerBaseDir + targetTenantId;

        HdfsToS3PathBuilder builder = new HdfsToS3PathBuilder(useEmr);
        sourceCustomerRoot = builder.getS3PathWithGlob(yarnConfiguration, sourceCustomerRoot + "/", false, s3Bucket);
        sourceCustomerRoot = StringUtils.removeEnd(sourceCustomerRoot, "/");
        try (PerformanceTimer timer = new PerformanceTimer("Copy hdfs data: Copy modeling data directory")) {
            ModelingHdfsUtils.copyModelingDataDirectory(sourceCustomerRoot, targetCustomerRoot, eventTableName,
                    cpEventTableName, yarnConfiguration, s3Bucket, useEmr);
        }

        String sourceModelRoot = sourceCustomerRoot + "/models/" + eventTableName + "/"
                + UuidUtils.extractUuid(modelSummary.getId());
        sourceModelRoot = builder.getS3PathWithGlob(yarnConfiguration, sourceModelRoot, false, s3Bucket);
        String sourceModelSummaryPath = ModelingHdfsUtils.findModelSummaryPath(yarnConfiguration, sourceModelRoot);
        String uuid = UUID.randomUUID().toString();
        String sourceModelDirPath = new Path(sourceModelSummaryPath).getParent().getParent().toString();
        String sourceModelLocalRoot = new Path(sourceModelDirPath).getName();
        String modelSummaryLocalPath = sourceModelLocalRoot + "/enhancements/modelsummary.json";

        try (PerformanceTimer timer = new PerformanceTimer("Copy hdfs data: Delete directory")) {
            FileUtils.deleteDirectory(new File(sourceModelLocalRoot));
        }
        try (PerformanceTimer timer = new PerformanceTimer("Copy hdfs data: Copy hdfs to local")) {
            log.info(String.format("Copying hdfs data from %s to %s", sourceModelDirPath, "."));
            HdfsUtils.copyHdfsToLocal(yarnConfiguration, sourceModelDirPath, ".");
        }

        Module module = null;
        if (StringUtils.isNotEmpty(modelSummary.getModuleName())) {
            module = metadataProxy.getModule(sourceTenantId, modelSummary.getModuleName());
        } else if (StringUtils.isNotEmpty(modelSummary.getPivotArtifactPath())) {
            String moduleName = StringUtils.substringBetween(modelSummary.getPivotArtifactPath(), //
                    PathBuilder.buildMetadataPath(CamilleEnvironment.getPodId(), //
                            CustomerSpace.parse(sourceTenantId)).toString() + "/", //
                    "/");
            module = metadataProxy.getModule(sourceTenantId, moduleName);
        }

        Map<String, Artifact> newArtifactsMap = new HashMap<>();
        String newModuleName = "cp_module_" + UUID.randomUUID().toString();
        if (module != null) {
            CustomerSpace customerSpace = CustomerSpace.parse(targetTenantId);
            try (PerformanceTimer timer = new PerformanceTimer("Copy hdfs data: Copy artifacts in module")) {
                newArtifactsMap = ModelingHdfsUtils.copyArtifactsInModule(yarnConfiguration, module.getArtifacts(),
                        customerSpace, newModuleName, s3Bucket, useEmr);
            }
            for (Artifact artifact : newArtifactsMap.values()) {
                metadataProxy.createArtifact(customerSpace.toString(), newModuleName, artifact.getName(), artifact);
            }
        }
        String contents = FileUtils.readFileToString(new File(modelSummaryLocalPath), "UTF-8");
        SourceFile sourceFile = sourceFileService.getByTableNameCrossTenant(cpTrainingTableName);

        JsonNode newModelSummary = null;
        try (PerformanceTimer timer = new PerformanceTimer("Copy hdfs data: Construct new model summary")) {
            newModelSummary = ModelingHdfsUtils.constructNewModelSummary(contents, targetTenantId, cpTrainingTableName,
                    cpEventTableName, uuid, modelSummary.getDisplayName(), newArtifactsMap, newModuleName, sourceFile);
        }

        String modelFileName = ModelingHdfsUtils.getModelFileNameFromLocalDir(sourceModelLocalRoot);
        JsonNode newModel = null;
        try (PerformanceTimer timer = new PerformanceTimer("Copy hdfs data: Construct new model")) {
            newModel = ModelingHdfsUtils.constructNewModel(sourceModelLocalRoot + "/" + modelFileName,
                    "ms__" + uuid + "-PLSModel");
        }
        try (PerformanceTimer timer = new PerformanceTimer("Copy hdfs data: Delete quietly and write")) {
            FileUtils.deleteQuietly(new File(sourceModelLocalRoot + "/enhancements/.modelsummary.json.crc"));
            FileUtils.write(new File(modelSummaryLocalPath), newModelSummary.toString(), "UTF-8", false);
        }

        String targetModelRoot = targetCustomerRoot + "/models/" + cpEventTableName + "/" + uuid;

        try (PerformanceTimer timer = new PerformanceTimer("Copy hdfs data: Delete quietly and write")) {
            FileUtils.deleteQuietly(new File(sourceModelLocalRoot + "/." + modelFileName + ".crc"));
            FileUtils.write(new File(sourceModelLocalRoot + "/" + modelFileName), newModel.toString(), "UTF-8", false);
        }

        String s3TargetModelRoot = builder.exploreS3FilePath(targetModelRoot, s3Bucket);
        try (PerformanceTimer timer = new PerformanceTimer("Copy hdfs data: mkdir")) {
            log.info(String.format("mkdir: %s", targetModelRoot));
            log.info(String.format("mkdir: %s", s3TargetModelRoot));
            HdfsUtils.mkdir(yarnConfiguration, targetModelRoot);
            HdfsUtils.mkdir(yarnConfiguration, s3TargetModelRoot);
        }
        try (PerformanceTimer timer = new PerformanceTimer("Copy hdfs data: Copy from local to hdfs")) {
            log.info(String.format("Copying hdfs data from %s to %s", sourceModelLocalRoot, targetModelRoot));
            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, sourceModelLocalRoot, targetModelRoot);
            log.info(String.format("Copying hdfs data from %s to %s", sourceModelLocalRoot, s3TargetModelRoot));
            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, sourceModelLocalRoot, s3TargetModelRoot);
        }
        try (PerformanceTimer timer = new PerformanceTimer("Copy hdfs data: Delete directory")) {
            FileUtils.deleteDirectory(new File(sourceModelLocalRoot));
        }

        return newModelSummary.get("ModelDetails").get("ModelID").asText();
    }
}

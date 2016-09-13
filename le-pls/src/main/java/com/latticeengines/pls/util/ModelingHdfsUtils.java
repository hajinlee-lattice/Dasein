package com.latticeengines.pls.util;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.pls.ModelSummary;

public class ModelingHdfsUtils {

    public static String findModelSummaryPath(Configuration config, String dir) throws IOException {
        List<String> paths = HdfsUtils.getFilesForDirRecursive(config, dir, new HdfsUtils.HdfsFileFilter() {

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

    public static JsonNode constructNewModel(String modelLocalPath, String modelId) throws IOException {
        String contents = FileUtils.readFileToString(new File(modelLocalPath), "UTF-8");
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode json = objectMapper.readTree(contents);

        ObjectNode summary = (ObjectNode) json.get("Summary");
        summary.put("ModelID", modelId);
        return json;
    }

    public static String getModelFileName(Configuration conf, String path) throws IOException {
        return new Path(getModelFilePath(conf, path)).getName();
    }

    public static String getModelFilePath(Configuration conf, String path) throws IOException {
        List<String> paths = HdfsUtils.getFilesForDir(conf, path, ".*.model.json");
        if (paths.size() == 0) {
            throw new LedpException(LedpCode.LEDP_00002);
        }
        return paths.get(0);
    }

    public static String getStandardDataComposition(Configuration conf, String sourceDataDir,
            final String eventTableName) throws IOException {
        List<String> paths = HdfsUtils.getFilesForDirRecursive(conf, sourceDataDir, new HdfsFileFilter() {
            @Override
            public boolean accept(FileStatus file) {
                return file.getPath().getName().equals("datacomposition.json") //
                        && file.getPath().getParent().getName().startsWith(eventTableName);
            }
        });
        return paths.get(0);
    }

    public static void copyModelingDataDirectory(String sourceCustomerRoot, String targetCustomerRoot,
            String eventTableName, String cpEventTableName, Configuration yarnConfiguration) throws IOException {
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

    public static String getModelFileNameFromLocalDir(String sourceModelLocalRoot) throws IllegalArgumentException,
            IOException {
        Configuration localFileSystemConfig = new Configuration();
        localFileSystemConfig.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        return ModelingHdfsUtils.getModelFileName(localFileSystemConfig, sourceModelLocalRoot);
    }

    public static JsonNode constructNewModelSummary(String contents, String targetTenantId, String cpTrainingTableName,
            String cpEventTableName, String uuid, String modelDisplayName, String newPivotMappingFilePath) throws IOException {
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
        if(StringUtils.isNotEmpty(newPivotMappingFilePath)){
            provenance.put("Pivot_Artifact_Path", newPivotMappingFilePath);
        }
        return json;
    }

    public static String getEventTableNameFromHdfs(Configuration yarnConfiguration, String customerModelBaseDir,
            String modelId) throws IOException {
        final String uuid = UuidUtils.extractUuid(modelId);
        List<String> paths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, customerModelBaseDir,
                new HdfsFileFilter() {

                    @Override
                    public boolean accept(FileStatus file) {
                        return file.getPath().getName().equals(uuid);
                    }
                });
        if (paths.size() == 0) {
            throw new LedpException(LedpCode.LEDP_00002);
        }
        return new Path(paths.get(0)).getParent().getName();
    }

    public static String copyPivotMappingFile(Configuration yarnConfiguration, ModelSummary modelSummary,
            String targetTenantId) throws IllegalArgumentException, IOException {
        String pivotMappingFilePath = modelSummary.getPivotArtifactPath();
        if (StringUtils.isEmpty(pivotMappingFilePath)) {
            return "";
        }
        com.latticeengines.domain.exposed.camille.Path newMetadataPath = PathBuilder.buildMetadataPath(
                CamilleEnvironment.getPodId(), CustomerSpace.parse(targetTenantId))
                .append(UUID.randomUUID().toString());
        HdfsUtils.mkdir(yarnConfiguration, newMetadataPath.toString());

        String newPivotMappingFilePath = newMetadataPath.append(ArtifactType.PivotMapping.getPathToken())
                .append(new Path(pivotMappingFilePath).getName()).toString();
        HdfsUtils.copyFiles(yarnConfiguration, pivotMappingFilePath, newPivotMappingFilePath);

        return newPivotMappingFilePath;
    }
}

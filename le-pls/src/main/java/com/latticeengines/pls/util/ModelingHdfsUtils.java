package com.latticeengines.pls.util;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;

public class ModelingHdfsUtils {

    public static String findModelSummaryPath(Configuration config, String dir) throws IOException {
        List<String> paths = HdfsUtils.getFilesForDirRecursive(config, dir, file -> {
            if (file == null) {
                return false;
            }
            String name = file.getPath().getName().toString();
            return name.equals("modelsummary.json");
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
        if (summary != null) {
            summary.put("ModelID", modelId);
        }
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
        List<String> paths = HdfsUtils.onlyGetFilesForDirRecursive(conf, sourceDataDir, file -> {
            return file.getPath().getName().equals("datacomposition.json") //
                    && file.getPath().getParent().getName().startsWith(eventTableName);
        }, true);

        return paths.get(0);
    }

    public static String getStandardDataCompositionWithRegex(Configuration conf, String sourceDataDir,
            final String eventTableName) throws IOException {
        List<String> paths = HdfsUtils.getFilesForDir(conf, sourceDataDir,
                String.format("^%s.*Metadata$", eventTableName));

        if (paths.size() == 0) {
            throw new LedpException(LedpCode.LEDP_00002);
        }

        return String.format("%s/datacomposition.json", paths.get(0));
    }

    public static void copyModelingDataDirectory(String sourceCustomerRoot, String targetCustomerRoot,
            String eventTableName, String cpEventTableName, Configuration yarnConfiguration) throws IOException {
        String sourceDataRoot = sourceCustomerRoot + "/data/" + eventTableName;
        String targetDataRoot = targetCustomerRoot + "/data/" + cpEventTableName;

        try (PerformanceTimer timer = new PerformanceTimer(
                "Copy hdfs data: Copy modeling data directory - copy " + "files")) {
            HdfsUtils.copyFiles(yarnConfiguration, sourceDataRoot, targetDataRoot);
        }

        String sourceStandardDataCompositionPath = null;
        try (PerformanceTimer timer = new PerformanceTimer(
                "Copy hdfs data: Copy modeling data directory - get " + "standard data composition")) {
            sourceStandardDataCompositionPath = ModelingHdfsUtils.getStandardDataCompositionWithRegex(yarnConfiguration,
                    sourceCustomerRoot + "/data/", eventTableName);

            Logger log = LoggerFactory.getLogger(ModelingHdfsUtils.class);
            log.info("sourceStandardDataCompositionPath is: " + sourceStandardDataCompositionPath);
        }

        String targetStandardDataCompositionPath = sourceStandardDataCompositionPath
                .replace(sourceCustomerRoot, targetCustomerRoot).replace(eventTableName, cpEventTableName);

        try (PerformanceTimer timer = new PerformanceTimer(
                "Copy hdfs data: Copy modeling data directory - copy " + "files")) {
            HdfsUtils.copyFiles(yarnConfiguration, new Path(sourceStandardDataCompositionPath).getParent().toString(),
                    new Path(targetStandardDataCompositionPath).getParent().toString());
        }
    }

    public static String getModelFileNameFromLocalDir(String sourceModelLocalRoot)
            throws IllegalArgumentException, IOException {
        Configuration localFileSystemConfig = new Configuration();
        localFileSystemConfig.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        return ModelingHdfsUtils.getModelFileName(localFileSystemConfig, sourceModelLocalRoot);
    }

    public static JsonNode constructNewModelSummary(String contents, String targetTenantId, String cpTrainingTableName,
            String cpEventTableName, String uuid, String modelDisplayName, Map<String, Artifact> artifactsMap,
            String newModuleName, SourceFile sourceFile) throws IOException {
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
        if (sourceFile != null) {
            provenance.put(ProvenancePropertyName.TrainingFilePath.getName(), sourceFile.getPath());
        }
        if (!artifactsMap.isEmpty()) {
            if (artifactsMap.containsKey(ArtifactType.PivotMapping.getCode())) {
                provenance.put("Pivot_Artifact_Path", artifactsMap.get(ArtifactType.PivotMapping.getCode()).getPath());
            }
            provenance.put("Module_Name", newModuleName);
        }
        return json;
    }

    public static String getEventTableNameFromHdfs(Configuration yarnConfiguration, String customerModelBaseDir,
            String modelId) throws IOException {
        final String uuid = UuidUtils.extractUuid(modelId);
        List<String> paths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, customerModelBaseDir, file -> {
            return file.getPath().getName().equals(uuid);
        });
        if (paths.size() == 0) {
            throw new LedpException(LedpCode.LEDP_00002);
        }
        return new Path(paths.get(0)).getParent().getName();
    }

    public static Map<String, Artifact> copyArtifactsInModule(Configuration yarnConfiguration, List<Artifact> artifacts,
            CustomerSpace customerSpace, String newModuleName) throws IllegalArgumentException, IOException {
        Map<String, Artifact> newArtifactsMap = new HashMap<>();

        for (Artifact artifact : artifacts) {
            ArtifactType artifactType = artifact.getArtifactType();
            com.latticeengines.domain.exposed.camille.Path path = PathBuilder.buildMetadataPathForArtifactType(
                    CamilleEnvironment.getPodId(), //
                    customerSpace, newModuleName, artifactType);
            String hdfsPath = String.format("%s/%s.%s", path.toString(), artifact.getName(),
                    artifactType.getFileType());
            HdfsUtils.copyFiles(yarnConfiguration, artifact.getPath(), hdfsPath);
            Artifact newArtifact = new Artifact();
            newArtifact.setPath(hdfsPath);
            newArtifact.setArtifactType(artifactType);
            newArtifact.setName(artifact.getName());
            newArtifactsMap.put(artifactType.getCode(), newArtifact);
        }
        return newArtifactsMap;
    }
}

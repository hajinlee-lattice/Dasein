package com.latticeengines.upgrade.yarn;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;

@Component("yarnManager")
public class YarnManager {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String MODEL_NAME = "PLSModel";
    private static final String TEMPFOLDER = "tmp";
    private static final String MS_PATH = "/enhancements/modelsummary.json";

    @Autowired
    protected Configuration yarnConfiguration;

    @Value("${dataplatform.customer.basedir}")
    protected String customerBase;

    public void createTupleIdCustomerRootIfNotExist(String customer) {
        String customerPath = YarnPathUtils.constructTupleIdCustomerRoot(customerBase, customer);
        if (!customerTupleIdPathExists(customer)) {
            try {
                HdfsUtils.mkdir(yarnConfiguration, customerPath);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_24000,
                        "Cannot create tuple id directory for customer " + customer, e);
            }
        }
    }

    public int moveModelsFromSingularToTupleId(String customer) {
        List<String> modelJsonPaths = findAllModelPathsInSingularId(customer);

        for (String jsonPath: modelJsonPaths) {
            String uuid = YarnPathUtils.extractUuid(jsonPath);
            String modelPathInSingular = findModelPathInSingular(customer, uuid);
            String modelPathInTuple = YarnPathUtils.substituteByTupleId(modelPathInSingular);
            copyHdfsToHdfs(modelPathInSingular, modelPathInTuple);
        }

        return modelJsonPaths.size();
    }

    public void fixModelNameInTupleId(String customer, String uuid) {
        String srcModelJsonFullPath = findModelPathInTuple(customer, uuid);
        if (!srcModelJsonFullPath.endsWith("model.json")) {
            String newModelJsonFullPath = srcModelJsonFullPath.replace(".json", "_model.json");
            String srcModelCsvFullPath = srcModelJsonFullPath.replace(".json", ".csv");
            String newModelCsvFullPath = srcModelJsonFullPath.replace(".json", "_model.csv");
            try {
                HdfsUtils.moveFile(yarnConfiguration, srcModelJsonFullPath, newModelJsonFullPath);
                HdfsUtils.moveFile(yarnConfiguration, srcModelCsvFullPath, newModelCsvFullPath);
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_24000, "Failed to move file from one src to dest path.", e);
            }
        }
    }

    public boolean modelJsonExistsInSingularId(String customer, String uuid) {
        try {
            String srcModelJsonFullPath = findModelPathInSingular(customer, uuid);
            return srcModelJsonFullPath != null;
        } catch (Exception e) {
            return false;
        }
    }

    public void deleteModelSummaryInTupleId(String customer, String uuid) {
        if (modelSummaryExistsInTupleId(customer, uuid)) {
            String modelFolder = findModelFolderPathInTuple(customer, uuid);
            String destPath = modelFolder + MS_PATH;
            try {
                HdfsUtils.rmdir(yarnConfiguration, destPath);
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_24000, "Failed to delete modelsummary.json", e);
            }
        }
    }

    public boolean modelSummaryExistsInSingularId(String customer, String uuid) {
        try {
            String modelFolder = findModelFolderPathInSingular(customer, uuid);
            String destPath = modelFolder + MS_PATH;
            return hdfsPathExists(destPath);
        } catch (LedpException e) {
            if (LedpCode.LEDP_24000.equals(e.getCode())) {
                return false;
            }
            throw e;
        }
    }

    public boolean modelSummaryExistsInTupleId(String customer, String uuid) {
        try {
            String modelFolder = findModelFolderPathInTuple(customer, uuid);
            String destPath = modelFolder + MS_PATH;
            return hdfsPathExists(destPath);
        } catch (LedpException e) {
            if (LedpCode.LEDP_24000.equals(e.getCode())) {
                return false;
            }
            throw e;
        }
    }

    public List<String> findAllUuidsInSingularId(String customer) {
        List<String> paths = findAllModelPathsInSingularId(customer);
        List<String> uuids = new ArrayList<>();
        for (String path: paths) {
            uuids.add(YarnPathUtils.parseUuid(path));
        }
        return uuids;
    }

    public List<String> findAllUuidsInTupleId(String customer) {
        List<String> paths = findAllModelPathsInTuplerId(customer);
        List<String> uuids = new ArrayList<>();
        for (String path: paths) {
            uuids.add(YarnPathUtils.parseUuid(path));
        }
        return uuids;
    }

    public void uploadModelsummary(String customer, String uuid, JsonNode summary) {
        String modelFolder = findModelFolderPathInTuple(customer, uuid);
        String path = modelFolder + MS_PATH;
        try {
            HdfsUtils.writeToFile(yarnConfiguration, path, summary.toString());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24000, "Failed to upload modelsummary.json", e);
        }
    }

    public JsonNode generateModelSummary(String customer, String uuid) {
        JsonNode json = readModelAsJson(customer, uuid);
        Long constructionTime = getTimestampFromModelJson(json);
        String srcModelJsonFullPath = findModelPathInSingular(customer, uuid);
        String eventTable = YarnPathUtils.parseEventTable(srcModelJsonFullPath);
        String lookupId = String.format("%s|%s|%s", customer, eventTable, YarnPathUtils.extractUuid(uuid));

        ObjectNode detail = objectMapper.createObjectNode();
        detail.put("Name", MODEL_NAME);
        detail.put("ModelID", YarnPathUtils.constructModelGuidFromUuid(uuid));
        detail.put("ConstructionTime", constructionTime/1000L);
        detail.put("LookupId", lookupId);

        ObjectNode summary = objectMapper.createObjectNode();
        summary.set("ModelDetails", detail);

        return summary;
    }

    public DateTime getModelCreationDate(String customer, String modelGuid) {
        String uuid = YarnPathUtils.extractUuid(modelGuid);
        JsonNode json = readModelAsJson(customer, uuid);
        Long constructionTime = getTimestampFromModelJson(json);
        return new DateTime(constructionTime);
    }

    private boolean customerTupleIdPathExists(String customer) {
        String customerPath = YarnPathUtils.constructTupleIdCustomerRoot(customerBase, customer);
        return hdfsPathExists(customerPath);
    }

    private boolean hdfsPathExists(String path) {
        try {
            return HdfsUtils.fileExists(yarnConfiguration, path);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24000,
                    "Cannot check if path " + path + " exists in hdfs.", e);
        }
    }

    private void copyHdfsToHdfs(String src, String dest) {
        String tmpLocalDir = TEMPFOLDER + "/" + UUID.randomUUID();
        try {
            HdfsUtils.copyHdfsToLocal(yarnConfiguration, src, tmpLocalDir);
            HdfsUtils.copyLocalToHdfs(yarnConfiguration, tmpLocalDir, dest);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24000, "Failed to copy file from one src to dest path.", e);
        } finally {
            FileUtils.deleteQuietly(new File(TEMPFOLDER));
        }
    }

    private Long getTimestampFromModelJson(JsonNode json) {
        JsonNode constructionJson = json.get("Summary").get("ConstructionInfo").get("ConstructionTime");
        ObjectMapper mapper = new ObjectMapper();
        try {
            ModelingMetadata.DateTime dateTime = mapper.treeToValue(constructionJson, ModelingMetadata.DateTime.class);
            return convertModelingTimestampToLong(dateTime);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24000, "Failed to parse construction time.", e);
        }
    }

    private JsonNode readModelAsJson(String customer, String uuid) {
        String srcModelJsonFullPath = findModelPathInSingular(customer, uuid);
        try {
            String jsonContent = HdfsUtils.getHdfsFileContents(yarnConfiguration, srcModelJsonFullPath);
            return objectMapper.readTree(jsonContent);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24000,
                    "Failed to read the content of model.json for model " + uuid, e);
        }
    }

    private String findModelFolderPathInTuple(String customer, String uuid) {
        String singularPath = findModelFolderPathInSingular(customer, uuid);
        return YarnPathUtils.substituteByTupleId(singularPath);
    }

    private String findModelFolderPathInSingular(String customer, String uuid) {
        String srcModelJsonFullPath = findModelPathInSingular(customer, uuid);
        String eventTable = YarnPathUtils.parseEventTable(srcModelJsonFullPath);
        String containerId = YarnPathUtils.parseContainerId(srcModelJsonFullPath);
        String modelsRoot = YarnPathUtils.constructSingularIdModelsRoot(customerBase, customer);
        return modelsRoot + "/" + eventTable + "/" + uuid + "/" + containerId;
    }

    private String findModelPathInTuple(String customer, String uuid) {
        List<String> paths = findAllModelPathsInTuplerId(customer);
        for (String path : paths) {
            if (path.contains(uuid))
                return path;
        }
        RuntimeException e = new RuntimeException("No model json with specific uuid can be found.");
        throw new LedpException(LedpCode.LEDP_24000, "Cannot find the path for model" + uuid, e);
    }

    private String findModelPathInSingular(String customer, String uuid) {
        List<String> paths = findAllModelPathsInSingularId(customer);
        for (String path : paths) {
            if (path.contains(uuid))
                return path;
        }
        RuntimeException e = new RuntimeException("No model json with specific uuid can be found.");
        throw new LedpException(LedpCode.LEDP_24000, "Cannot find the path for model" + uuid, e);
    }

    private List<String> findAllModelPathsInSingularId(String customer) {
        String modelsRoot = YarnPathUtils.constructSingularIdModelsRoot(customerBase, customer);
        try {
            return findAllModelPaths(modelsRoot);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24000, "Cannot find all model jsons for customer " + customer, e);
        }
    }

    private List<String> findAllModelPathsInTuplerId(String customer) {
        String modelsRoot = YarnPathUtils.constructTupleIdModelsRoot(customerBase, customer);
        try {
            return findAllModelPaths(modelsRoot);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_24000, "Cannot find all model jsons for customer " + customer, e);
        }
    }

    private List<String> findAllModelPaths(String modelsRoot) throws Exception {
        if (!hdfsPathExists(modelsRoot)) {
            return new ArrayList<>();
        }

        return HdfsUtils.getFilesForDirRecursive(yarnConfiguration, modelsRoot,
            new HdfsUtils.HdfsFileFilter() {
                @Override
                public boolean accept(FileStatus fileStatus) {
                    if (fileStatus == null) {
                        return false;
                    }
                    Pattern p = Pattern.compile(".*json");
                    String filePath = fileStatus.getPath().getName();
                    Matcher matcher = p.matcher(filePath);
                    return (matcher.matches() && !shouldExclude(filePath));
                }

                private boolean shouldExclude(String path) {
                    List<String> blacklist = Arrays.asList(
                            "enhancements",
                            "modelsummary",
                            "diagnostics",
                            "DataComposition",
                            "ScoreDerivation"
                    );
                    for (String token: blacklist) {
                        if (path.contains(token)) return true;
                    }
                    return false;
                }
            });
    }

    private Long convertModelingTimestampToLong(ModelingMetadata.DateTime dateTime) {
        Pattern pattern = Pattern.compile("\\d+");
        Matcher matcher = pattern.matcher(dateTime.getDateTime());
        if (matcher.find()) {
            return Long.valueOf(matcher.group(0));
        } else {
            return 0L;
        }
    }

}

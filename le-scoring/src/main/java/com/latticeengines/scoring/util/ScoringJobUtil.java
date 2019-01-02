package com.latticeengines.scoring.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;

public class ScoringJobUtil {

    public static List<String> findModelUrlsToLocalize(Configuration yarnConfiguration, String tenant,
            String customerBaseDir, List<String> modelGuids, boolean useScoreDerivation) {
        List<String> modelFilePaths = findAllModelPathsInHdfs(yarnConfiguration, tenant, customerBaseDir);
        return findModelUrlsToLocalize(yarnConfiguration, tenant, modelGuids, modelFilePaths, useScoreDerivation);
    }

    public static List<String> findAllModelPathsInHdfs(Configuration yarnConfiguration, String tenant,
            String customerBaseDir) {
        String customerModelPath = customerBaseDir + "/" + tenant + "/models";
        List<String> modelFilePaths = getModelFiles(yarnConfiguration, customerModelPath);
        if (CollectionUtils.isEmpty(modelFilePaths)) {
            throw new LedpException(LedpCode.LEDP_20008, new String[] { tenant });
        }
        return modelFilePaths;
    }

    private static List<String> getModelFiles(Configuration yarnConfiguration, String hdfsDir) {
        List<String> modelFilePaths;
        try {
            modelFilePaths = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, hdfsDir,
                    fileStatus -> {
                        if (fileStatus == null) {
                            return false;
                        }
                        Pattern p = Pattern.compile(".*model" + ScoringDaemonService.JSON_SUFFIX);
                        Matcher matcher = p.matcher(fileStatus.getPath().getName());
                        return matcher.matches();
                    });
        } catch (Exception e) {
            throw new RuntimeException("Failed to check model.json in " + hdfsDir, e);
        }
        return modelFilePaths;
    }

    @VisibleForTesting
    static List<String> findModelUrlsToLocalize(Configuration yarnConfiguration, String tenant, List<String> modelGuids,
            List<String> modelFilePaths, boolean useScoreDerivation) {
        List<String> modelUrlsToLocalize = new ArrayList<>();
        label: for (String modelGuid : modelGuids) {
            String uuid = UuidUtils.extractUuid(modelGuid);
            for (String path : modelFilePaths) {
                if (uuid.equals(UuidUtils.parseUuid(path))) {
                    try {
                        HdfsUtils.getCheckSum(yarnConfiguration, path);
                    } catch (IOException e) {
                        throw new LedpException(LedpCode.LEDP_20021, new String[] { path, tenant });
                    }
                    modelUrlsToLocalize.add(path + "#" + uuid);
                    if (useScoreDerivation) {
                        modelUrlsToLocalize.add(new Path(path).getParent() + "/enhancements/scorederivation.json" + "#"
                                + uuid + "_scorederivation");
                    }
                    continue label;
                }
            }
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelGuid });
        }
        return modelUrlsToLocalize;
    }

    public static JsonNode generateDataTypeSchema(Schema schema) {
        List<Field> fields = schema.getFields();
        ObjectNode jsonObj = new ObjectMapper().createObjectNode();
        for (Field field : fields) {
            String type = AvroUtils.getType(field).getName();
            if (type.equals("string") || type.equals("bytes"))
                jsonObj.put(field.name(), 1);
            else
                jsonObj.put(field.name(), 0);
        }
        return jsonObj;
    }

    public static Table createGenericOutputSchema(String uniqueKeyColumn, boolean hasRevenue, boolean isCdl) {
        Table scoreResultTable = new Table();
        String tableName = "ScoreResult";
        scoreResultTable.setName(tableName);
        Attribute id = new Attribute();
        id.setName(uniqueKeyColumn);
        id.setDisplayName(uniqueKeyColumn);
        if (InterfaceName.AnalyticPurchaseState_ID.name().equals(uniqueKeyColumn)) {
            id.setPhysicalDataType(Type.LONG.name());
        } else {
            id.setPhysicalDataType(Type.STRING.name());
        }
        id.setSourceLogicalDataType(InterfaceName.Id.name());

        Attribute modelId = new Attribute();
        modelId.setName(ScoreResultField.ModelId.displayName);
        modelId.setDisplayName(ScoreResultField.ModelId.displayName);
        modelId.setPhysicalDataType(ScoreResultField.ModelId.physicalDataType);
        modelId.setSourceLogicalDataType(ScoreResultField.ModelId.sourceLogicalDataType);

        Attribute percentile = new Attribute();
        percentile.setName(ScoreResultField.Percentile.displayName);
        percentile.setDisplayName(ScoreResultField.Percentile.displayName);
        percentile.setPhysicalDataType(ScoreResultField.Percentile.physicalDataType);
        percentile.setSourceLogicalDataType(ScoreResultField.Percentile.sourceLogicalDataType);

        Attribute rawScore = createAttribute(ScoreResultField.RawScore);
        List<Attribute> attributes = new ArrayList<>();
        attributes.add(id);
        attributes.add(modelId);
        attributes.add(percentile);
        attributes.add(rawScore);

        if (InterfaceName.AnalyticPurchaseState_ID.name().equals(uniqueKeyColumn)
                || InterfaceName.__Composite_Key__.name().equals(uniqueKeyColumn)) {
            Attribute probility = createAttribute(ScoreResultField.Probability);
            attributes.add(probility);
            Attribute normalized = createAttribute(ScoreResultField.NormalizedScore);
            attributes.add(normalized);
        }
        if (isCdl) {
            Attribute predictedRevenue = createAttribute(ScoreResultField.PredictedRevenue);
            attributes.add(predictedRevenue);
            Attribute expectedRevenue = createAttribute(ScoreResultField.ExpectedRevenue);
            attributes.add(expectedRevenue);
            if (!hasRevenue) {
                predictedRevenue.setNullable(Boolean.TRUE);
                expectedRevenue.setNullable(Boolean.TRUE);
            }
        }

        scoreResultTable.setAttributes(attributes);
        return scoreResultTable;
    }

    private static Attribute createAttribute(ScoreResultField field) {
        Attribute attribute = new Attribute();
        attribute.setName(field.name());
        attribute.setDisplayName(field.name());
        attribute.setPhysicalDataType(field.physicalDataType);
        attribute.setSourceLogicalDataType(field.sourceLogicalDataType);
        return attribute;
    }

    public static List<String> getCacheFiles(Configuration yarnConfiguration, String currentVersionInStack)
            throws IOException {
        List<String> files = new ArrayList<>();
        String dependencyPath = "/app/";
        String jarDependencyPath = "/scoring/lib";
        String scoringPythonPath = "/scoring/scripts/scoring.py";
        String pythonLauncherPath = "/dataplatform/scripts/pythonlauncher.sh";
        String log4jXmlPath = "/conf/log4j2-yarn.xml";

        files.add(dependencyPath + currentVersionInStack + scoringPythonPath);
        files.add(dependencyPath + currentVersionInStack + pythonLauncherPath);
        files.addAll(HdfsUtils.getFilesForDir(yarnConfiguration,
                dependencyPath + currentVersionInStack + jarDependencyPath, ".*\\.jar$"));
        files.add(dependencyPath + currentVersionInStack + log4jXmlPath);
        return files;

    }

}

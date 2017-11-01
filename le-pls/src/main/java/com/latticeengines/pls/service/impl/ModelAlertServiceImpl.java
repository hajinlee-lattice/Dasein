package com.latticeengines.pls.service.impl;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelAlerts;
import com.latticeengines.pls.service.HdfsFileDownloader;
import com.latticeengines.pls.service.ModelAlertService;
import com.latticeengines.pls.service.impl.HdfsFileDownloaderImpl.DownloadBuilder;

@Component("modelAlertService")
public class ModelAlertServiceImpl implements ModelAlertService {

    private static final String TOP_PREDICTOR_CSV_FILE_SUFFIX = ".*_model.csv";
    private static final String TOP_PREDICTOR_CSV_FILE_NULL_VALUE_1 = "\"[\"\"NULL\"\"]\"";
    private static final String TOP_PREDICTOR_CSV_FILE_NULL_VALUE_2 = "\"[\"\"NONE\"\"]\"";
    private static final String TOP_PREDICTOR_CSV_FILE_NULL_VALUE_3 = "NOT AVAILABLE";
    private static final String TOP_PREDICTOR_CSV_FILE_NULL_VALUE_4 = "\"[\"\"NOT AVAILABLE\"\"]\"";
    private static final String DATA_DIAGNOSTICS_JSON_FILE_NAME = "diagnostics.json";
    private static final String METADATA_DIAGNOSTICS_JSON_FILE_NAME = "metadata-diagnostics.json";
    private static final String MODEL_SUMMARY_JSON_FILE_NAME = "modelsummary.json";
    private static final String RANDOM_FOREST_MODEL_TXT_FILE_NAME = ".*rf_model.txt";

    private static final String MODEL_SUMMARY_MODEL_DETAILS = "ModelDetails";
    private static final String MODEL_DETAILS_TOTAL_LEADS = "TotalLeads";
    private static final String MODEL_DETAILS_TOTAL_CONVERSIONS = "TotalConversions";
    private static final String MODEL_DETAILS_ROC_SCORE = "RocScore";

    private static final String DATA_DIAGNOSTICS_SUMMARY = "Summary";
    private static final String DATA_DIAGNOSTICS_GT200_DISCRETE_VALUE = "GT200_DiscreteValue";

    private static final String METADATA_DIAGNOSTICS_APPROVED_USAGE_ANNOTATION_ERRORS = "ApprovedUsageAnnotationErrors";
    private static final String METADATA_DIAGNOSTICS_TAGS_ANNOTATION_ERRORS = "TagsAnnotationErrors";
    private static final String METADATA_DIAGNOSTICS_CATEGORY_ANNOTATION_ERRORS = "CategoryAnnotationErrors";
    private static final String METADATA_DIAGNOSTICS_DISPLAY_NAME_ANNOTATION_ERRORS = "DisplayNameAnnotationErrors";
    private static final String METADATA_DIAGNOSTICS_STAT_TYPE_ANNOTATION_ERRORS = "StatisticalTypeAnnotationErrors";
    private static final String METADATA_DIAGNOSTICS_KEY = "key";

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${pls.modelalerts.min.success.events}")
    private long minSuccessEvents;

    @Value("${pls.modelalerts.min.conversion.percentage}")
    private double minConversionPercentage;

    @Value("${pls.modelalerts.min.rocscore}")
    private double minRocScore;

    @Value("${pls.modelalerts.max.rocscore}")
    private double maxRocScore;

    @Value("${pls.modelalerts.max.discrete.values}")
    private long maxNumberOfDiscreteValues;

    @Value("${pls.modelalerts.max.feature.importance}")
    private double maxFeatureImportance;

    @Value("${pls.modelalerts.max.null.lift}")
    private double maxLiftForNull;

    private static class ModelSummaryInfo {

        private Long totalLeads;
        private Long totalConversions;
        private Double rocScore;

        private ModelSummaryInfo(Long totalLeads, Long totalConversions, Double rocScore) {
            this.totalLeads = totalLeads;
            this.totalConversions = totalConversions;
            this.rocScore = rocScore;
        }

        private Long getTotalLeads() {
            return this.totalLeads;
        }

        private Long getTotalConversions() {
            return this.totalConversions;
        }

        private Double getRocScore() {
            return this.rocScore;
        }
    }

    @Override
    public ModelAlerts.ModelQualityWarnings generateModelQualityWarnings(String tenantId, String modelId) {
        ModelAlerts.ModelQualityWarnings modelQualityWarnings = new ModelAlerts.ModelQualityWarnings();
        try {
            ModelSummaryInfo modelSummaryInfo = getInfoFromModelSummary(tenantId, modelId);
            Long totalLeads = modelSummaryInfo.getTotalLeads();
            Long totalConversions = modelSummaryInfo.getTotalConversions();
            Double conversionRate = ((double) totalConversions / totalLeads) * 100;

            if (totalConversions < minSuccessEvents) {
                modelQualityWarnings.setLowSuccessEvents(totalConversions);
                modelQualityWarnings.setMinSuccessEvents(minSuccessEvents);
            }

            if (conversionRate < minConversionPercentage) {
                modelQualityWarnings.setLowConversionPercentage(conversionRate);
                modelQualityWarnings.setMinConversionPercentage(minConversionPercentage);
            }

            Double rocScore = modelSummaryInfo.getRocScore();
            if (rocScore < minRocScore || rocScore > maxRocScore) {
                modelQualityWarnings.setOutOfRangeRocScore(rocScore);
                modelQualityWarnings.setMinRocScore(minRocScore);
                modelQualityWarnings.setMaxRocScore(maxRocScore);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18041, e);
        }

        try {
            List<String> excessiveDiscreteValuesAttributes = getExcessiveDiscreteValues(tenantId, modelId);
            if (excessiveDiscreteValuesAttributes.size() != 0) {
                modelQualityWarnings.setExcessiveDiscreteValuesAttributes(excessiveDiscreteValuesAttributes);
                modelQualityWarnings.setMaxNumberOfDiscreteValues(maxNumberOfDiscreteValues);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18042, e);
        }

        try {
            List<Map.Entry<String, String>> excessivePredictiveAttributes = getExcessivePredictiveAttributes(tenantId,
                    modelId);
            if (excessivePredictiveAttributes.size() != 0) {
                modelQualityWarnings.setExcessivePredictiveAttributes(excessivePredictiveAttributes);
                modelQualityWarnings.setMaxFeatureImportance(maxFeatureImportance);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18043, e);
        }

        try {
            List<Map.Entry<String, String>> excessivePredictiveNullValuesAttributes = getExcessivePredictiveNullValuesAttributes(
                    tenantId, modelId);
            if (excessivePredictiveNullValuesAttributes.size() != 0) {
                modelQualityWarnings
                        .setExcessivePredictiveNullValuesAttributes(excessivePredictiveNullValuesAttributes);
                modelQualityWarnings.setMaxLiftForNull(maxLiftForNull);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18044, e);
        }
        return modelQualityWarnings;
    }

    private ModelSummaryInfo getInfoFromModelSummary(String tenantId, String modelId) throws Exception {
        ObjectMapper parser = new ObjectMapper();
        String modelSummaryContents = getFileContents(tenantId, modelId, MODEL_SUMMARY_JSON_FILE_NAME);

        JsonNode modelSummaryObject = parser.readTree(modelSummaryContents);
        JsonNode modelDetails = modelSummaryObject.get(MODEL_SUMMARY_MODEL_DETAILS);
        Long totalLeads = modelDetails.get(MODEL_DETAILS_TOTAL_LEADS).asLong();
        Long totalConversions = modelDetails.get(MODEL_DETAILS_TOTAL_CONVERSIONS).asLong();
        Double rocScore = modelDetails.get(MODEL_DETAILS_ROC_SCORE).asDouble();

        ModelSummaryInfo result = new ModelSummaryInfo(totalLeads, totalConversions, rocScore);
        return result;
    }

    private List<String> getExcessiveDiscreteValues(String tenantId, String modelId) throws Exception {
        ObjectMapper parser = new ObjectMapper();
        List<String> excessiveDiscreteValuesAttributes = new ArrayList<String>();
        String dataDiagnosticsContents = getFileContents(tenantId, modelId, DATA_DIAGNOSTICS_JSON_FILE_NAME);

        JsonNode dataDiagnosticsObject = parser.readTree(dataDiagnosticsContents);
        JsonNode summaryObject = dataDiagnosticsObject.get(DATA_DIAGNOSTICS_SUMMARY);
        ArrayNode gt200DiscreteValueArray = (ArrayNode) summaryObject.get(DATA_DIAGNOSTICS_GT200_DISCRETE_VALUE);

        for (Object columnNameObject : gt200DiscreteValueArray) {
            excessiveDiscreteValuesAttributes.add(columnNameObject.toString());
        }
        return excessiveDiscreteValuesAttributes;
    }

    private List<Map.Entry<String, String>> getExcessivePredictiveAttributes(String tenantId, String modelId)
            throws Exception {
        List<Map.Entry<String, String>> returnList = new ArrayList<Map.Entry<String, String>>();
        String rfModelContents = getFileContents(tenantId, modelId, RANDOM_FOREST_MODEL_TXT_FILE_NAME);

        String[] rows = rfModelContents.split("\n");
        for (int i = 0; i < rows.length; i++) {
            if (i == 0) {// skip the column name
                continue;
            }
            String[] kv = rows[i].split(",");
            if (Double.parseDouble(kv[1]) > maxFeatureImportance) {
                Map.Entry<String, String> entry = new AbstractMap.SimpleEntry<String, String>(kv[0], kv[1]);
                returnList.add(entry);
            }
        }
        return returnList;
    }

    private List<Map.Entry<String, String>> getExcessivePredictiveNullValuesAttributes(String tenantId, String modelId)
            throws Exception {
        List<Map.Entry<String, String>> returnList = new ArrayList<Map.Entry<String, String>>();
        String topPredictorContents = getFileContents(tenantId, modelId, TOP_PREDICTOR_CSV_FILE_SUFFIX);
        List<String> possibleNullValueList = Arrays.asList(TOP_PREDICTOR_CSV_FILE_NULL_VALUE_1,
                TOP_PREDICTOR_CSV_FILE_NULL_VALUE_2, TOP_PREDICTOR_CSV_FILE_NULL_VALUE_3,
                TOP_PREDICTOR_CSV_FILE_NULL_VALUE_4);

        String[] rows = topPredictorContents.split("\n");
        for (int i = 0; i < rows.length; i++) {
            if (i == 0) {// skip the column name
                continue;
            }
            String[] row = rows[i].split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            String attributeName = row[1];
            String attributeValue = row[5];
            String lift = row[7];
            if (possibleNullValueList.contains(attributeValue.toUpperCase())
                    && Double.parseDouble(lift) > maxLiftForNull) {
                Map.Entry<String, String> entry = new AbstractMap.SimpleEntry<String, String>(attributeName, lift);
                returnList.add(entry);
            }
        }
        return returnList;
    }

    @Override
    public ModelAlerts.MissingMetaDataWarnings generateMissingMetaDataWarnings(String tenantId, String modelId) {
        ModelAlerts.MissingMetaDataWarnings missingMetaDataWarnings = new ModelAlerts.MissingMetaDataWarnings();

        try {
            ObjectMapper parser = new ObjectMapper();
            String metadataDiagnosticsContents = getFileContents(tenantId, modelId, METADATA_DIAGNOSTICS_JSON_FILE_NAME);

            JsonNode metadataDiagnosticsObject = parser.readTree(metadataDiagnosticsContents);
            ArrayNode approvedUsageErrors = (ArrayNode) metadataDiagnosticsObject
                    .get(METADATA_DIAGNOSTICS_APPROVED_USAGE_ANNOTATION_ERRORS);
            ArrayNode tagsErrors = (ArrayNode) metadataDiagnosticsObject
                    .get(METADATA_DIAGNOSTICS_TAGS_ANNOTATION_ERRORS);
            ArrayNode categoryErrors = (ArrayNode) metadataDiagnosticsObject
                    .get(METADATA_DIAGNOSTICS_CATEGORY_ANNOTATION_ERRORS);
            ArrayNode displayErrors = (ArrayNode) metadataDiagnosticsObject
                    .get(METADATA_DIAGNOSTICS_DISPLAY_NAME_ANNOTATION_ERRORS);
            ArrayNode statTypeErrors = (ArrayNode) metadataDiagnosticsObject
                    .get(METADATA_DIAGNOSTICS_STAT_TYPE_ANNOTATION_ERRORS);

            List<String> invalidApprovedUsageAttributes = fillListFromJsonArray(approvedUsageErrors);
            List<String> invalidTagsAttributes = fillListFromJsonArray(tagsErrors);
            List<String> invalidCategoryAttributes = fillListFromJsonArray(categoryErrors);
            List<String> invalidDisplayNameAttributes = fillListFromJsonArray(displayErrors);
            List<String> invalidStatisticalTypeAttributes = fillListFromJsonArray(statTypeErrors);

            missingMetaDataWarnings.setInvalidApprovedUsageMissingAttributes(invalidApprovedUsageAttributes);
            missingMetaDataWarnings.setInvalidTagsAttributes(invalidTagsAttributes);
            missingMetaDataWarnings.setInvalidCategoryAttributes(invalidCategoryAttributes);
            missingMetaDataWarnings.setInvalidDisplayNameAttributes(invalidDisplayNameAttributes);
            missingMetaDataWarnings.setInvalidStatisticalTypeAttributes(invalidStatisticalTypeAttributes);

        } catch (Exception e) {
            if ((e instanceof LedpException) && ((LedpException) e).getCode().equals(LedpCode.LEDP_18023)) {
                // metadata-diagnostics.json file could be missing if metadata
                // validation passes
            } else {
                throw new LedpException(LedpCode.LEDP_18045, e);
            }
        }
        return missingMetaDataWarnings;
    }

    private List<String> fillListFromJsonArray(ArrayNode array) {
        List<String> returnList = new ArrayList<String>();
        if (array == null) {
            return returnList;
        }
        for (int i = 0; i < array.size(); i++) {
            returnList.add(array.get(i).get(METADATA_DIAGNOSTICS_KEY).asText());
        }
        return returnList;
    }

    private String getFileContents(String tenantId, String modelId, String filter) throws Exception {
        HdfsFileDownloader downloader = getDownloader();
        return downloader.getFileContents(tenantId, modelId, filter);
    }

    private HdfsFileDownloaderImpl getDownloader() {
        DownloadBuilder builder = new DownloadBuilder();
        builder.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir).setYarnConfiguration(yarnConfiguration);
        return new HdfsFileDownloaderImpl(builder);
    }

}

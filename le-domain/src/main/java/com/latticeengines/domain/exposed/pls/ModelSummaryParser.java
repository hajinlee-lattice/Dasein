package com.latticeengines.domain.exposed.pls;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.KeyValue;

public class ModelSummaryParser {

    public static final String NAME = "Name";
    public static final String MODEL_SUMMARY_PREDICTORS = "Predictors";
    public static final String MODEL_SUMMARY_SEGMENTATIONS = "Segmentations";
    public static final String DEFAULT_PREDICTOR_NAME = "DefaultPredictorName";
    private static final Logger log = LoggerFactory.getLogger(ModelSummaryParser.class);
    private static final String PREDICTOR_DISPLAY_NAME = "DisplayName";

    private static final String DEFAULT_PREDICTOR_DISPLAY_NAME = "DefaultDisplayName";

    private static final String PREDICTOR_APPROVED_USAGE = "ApprovedUsage";

    private static final String DEFAULT_PREDICTOR_APPROVED_USAGE = "None";

    private static final String PREDICTOR_CATEGORY = "Category";

    private static final String DEFAULT_PREDICTOR_CATEGORY = "DefaultCategory";

    private static final String PREDICTOR_FUNDAMENTAL_TYPE = "FundamentalType";

    private static final String DEFAULT_PREDICTOR_FUNDAMENTAL_TYPE = "Unknown";

    private static final String PREDICTOR_UNCERTAINTY_COEFFICIENT = "UncertaintyCoefficient";

    private static final Double DEFAULT_PREDICTOR_UNCERTAINTY_COEFFICIENT = 0D;

    private final int defaultBiPredictorNum;

    public ModelSummaryParser() {
        defaultBiPredictorNum = 50;
    }

    public ModelSummaryParser(int defaultBiPredictorNum) {
        this.defaultBiPredictorNum = defaultBiPredictorNum;
    }

    public ModelSummary parse(String hdfsPath, String fileContents) {

        if (fileContents == null) {
            return null;
        }

        // parse ModelSummary
        ModelSummary summary = new ModelSummary();
        try {
            KeyValue keyValue = new KeyValue();
            keyValue.setData(CompressionUtils.compressByteArray(fileContents.getBytes()));
            summary.setDetails(keyValue);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18020, new String[] { hdfsPath });
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode json;
        try {
            json = mapper.readValue(fileContents, JsonNode.class);
        } catch (IOException e) {
            // ignore
            return null;
        }

        JsonNode details = json.get("ModelDetails");

        String name = JsonUtils.getOrDefault(details.get("Name"), String.class, "PLS");
        Long constructionTime;
        try {
            long currentMillis = details.get("ConstructionTime").asLong() * 1000;
            getDate(currentMillis, "MM/dd/yyyy hh:mm:ss z");
            constructionTime = currentMillis;
        } catch (Exception e) {
            constructionTime = System.currentTimeMillis();
        }
        String lookupId = JsonUtils.getOrDefault(details.get("LookupID"), String.class, "");
        summary.setName(
                String.format("%s-%s", name.replace(' ', '_'), getDate(constructionTime, "MM/dd/yyyy hh:mm:ss z")));
        summary.setDisplayName(JsonUtils.getOrDefault(details.get("DisplayName"), String.class, ""));
        summary.setLookupId(lookupId);
        summary.setRocScore(JsonUtils.getOrDefault(details.get("RocScore"), Double.class, 0.0));
        summary.setTrainingRowCount(JsonUtils.getOrDefault(details.get("TrainingLeads"), Long.class, 0L));
        summary.setTestRowCount(JsonUtils.getOrDefault(details.get("TestingLeads"), Long.class, 0L));
        summary.setTotalRowCount(JsonUtils.getOrDefault(details.get("TotalLeads"), Long.class, 0L));
        summary.setTrainingConversionCount(JsonUtils.getOrDefault(details.get("TrainingConversions"), Long.class, 0L));
        summary.setTestConversionCount(JsonUtils.getOrDefault(details.get("TestingConversions"), Long.class, 0L));
        summary.setTotalConversionCount(JsonUtils.getOrDefault(details.get("TotalConversions"), Long.class, 0L));
        summary.setConstructionTime(constructionTime);
        summary.setModelType(
                JsonUtils.getOrDefault(details.get("ModelType"), String.class, ModelType.PYTHONMODEL.getModelType()));
        if (summary.getConstructionTime() == null) {
            summary.setConstructionTime(System.currentTimeMillis());
        }
        summary.setLastUpdateTime(summary.getConstructionTime());
        summary.setIncomplete(isIncomplete(json));
        setLiftStatistics(json.get(MODEL_SUMMARY_SEGMENTATIONS), summary);

        if (details.has("ModelID")) {
            summary.setId(details.get("ModelID").asText());
        } else if (details.has("LookupID")) {
            String uuid = details.get("LookupID").asText().split("\\|")[2];
            summary.setId(String.format("ms__%s-%s", uuid, name));
        } else {
            String uuid = UUID.randomUUID().toString();
            summary.setId(String.format("ms__%s-%s", uuid, name));
        }

        if (details.has("AverageRevenue")) {
            summary.setAverageRevenue(JsonUtils.getOrDefault(details.get("AverageRevenue"), Double.class, null));
        }

        JsonNode eventTableProvenance = json.get("EventTableProvenance");
        if (eventTableProvenance != null) {
            summary.setEventTableName(
                    JsonUtils.getOrDefault(eventTableProvenance.get("EventTableName"), String.class, ""));
            summary.setSourceSchemaInterpretation(
                    JsonUtils.getOrDefault(eventTableProvenance.get("SourceSchemaInterpretation"), String.class, ""));
            summary.setTrainingTableName(
                    JsonUtils.getOrDefault(eventTableProvenance.get("TrainingTableName"), String.class, ""));
            summary.setTargetTableName(
                    JsonUtils.getOrDefault(eventTableProvenance.get("TargetTableName"), String.class, ""));
            summary.setPivotArtifactPath(JsonUtils.getOrDefault(
                    eventTableProvenance.get(ProvenancePropertyName.PivotFilePath.getName()), String.class, null));
            summary.setModuleName(JsonUtils.getOrDefault(eventTableProvenance.get("Module_Name"), String.class, ""));

            ModelSummaryProvenance configuration = new ModelSummaryProvenance();
            configuration.setBoolean(ProvenancePropertyName.IsOneLeadPerDomain,
                    JsonUtils.getOrDefault(eventTableProvenance.get("Is_One_Lead_Per_Domain"), Boolean.class, false));
            configuration.setBoolean(ProvenancePropertyName.ExcludePropdataColumns,
                    JsonUtils.getOrDefault(eventTableProvenance.get("Exclude_Propdata_Columns"), Boolean.class, false));
            configuration.setBoolean(ProvenancePropertyName.ExcludePublicDomains,
                    JsonUtils.getOrDefault(eventTableProvenance.get("Exclude_Public_Domains"), Boolean.class, false));
            configuration.setString(ProvenancePropertyName.TrainingFilePath,
                    JsonUtils.getOrDefault(eventTableProvenance.get("Training_File_Path"), String.class, ""));
            if (eventTableProvenance.get(ProvenancePropertyName.PmmlFilePath.getName()) != null) {
                configuration.setString(ProvenancePropertyName.PmmlFilePath, JsonUtils.getOrDefault(
                        eventTableProvenance.get(ProvenancePropertyName.PmmlFilePath.getName()), String.class, ""));
            }
            configuration.setString(ProvenancePropertyName.TransformationGroupName,
                    JsonUtils.getOrDefault(eventTableProvenance.get("Transformation_Group_Name"), String.class,
                            TransformationGroup.STANDARD.getName()));
            configuration.setLong(ProvenancePropertyName.WorkflowJobId,
                    JsonUtils.getOrDefault(eventTableProvenance.get("Workflow_Job_Id"), Long.class, -1L));
            configuration.setBoolean(ProvenancePropertyName.IsV2ProfilingEnabled,
                    JsonUtils.getOrDefault(eventTableProvenance.get("Is_V2_Profiling_Enabled"), Boolean.class, false));
            configuration.setBoolean(ProvenancePropertyName.FuzzyMatchingEnabled,
                    JsonUtils.getOrDefault(eventTableProvenance.get("Fuzzy_Matching_Enabled"), Boolean.class, false));
            configuration.setBoolean(ProvenancePropertyName.ConflictWithOptionalRules, JsonUtils
                    .getOrDefault(eventTableProvenance.get("Conflict_With_Optional_Rules"), Boolean.class, false));
            configuration.setString(ProvenancePropertyName.RefineAndCloneParentModelId, JsonUtils
                    .getOrDefault(eventTableProvenance.get("Refine_And_Clone_Parent_Model_Id"), String.class, ""));

            summary.setModelSummaryConfiguration(configuration);
        }

        // the Id will be used to find hdfs path, make sure they are in sync.
        try {
            String uuidInPath = UuidUtils.parseUuid(hdfsPath);
            String uuidInId = UuidUtils.extractUuid(summary.getId());
            if (!uuidInPath.equals(uuidInId)) {
                summary.setId("ms__" + uuidInPath + "-PLSModel");
            }
        } catch (Exception e) {
            // ignore
        }

        try {
            if (json.has("Tenant")) {
                summary.setTenant(mapper.treeToValue(json.get("Tenant"), Tenant.class));
            } else if (details.has("Tenant")) {
                summary.setTenant(mapper.treeToValue(details.get("Tenant"), Tenant.class));
            } else {
                Tenant tenant = new Tenant();
                tenant.setPid(-1L);
                tenant.setRegisteredTime(System.currentTimeMillis());
                tenant.setId("FAKE_TENANT");
                tenant.setName("Fake Tenant");
                summary.setTenant(tenant);
            }
        } catch (JsonProcessingException e) {
            // ignore
        }

        // parse predictors
        JsonNode predictorsJsonNode = json.get(MODEL_SUMMARY_PREDICTORS);
        List<Predictor> predictors = parsePredictors(predictorsJsonNode, summary);
        summary.setPredictors(predictors);

        JsonNode crossValidatedMeanOfModelAccuracy = json.get("CrossValidatedMeanOfModelAccuracy");
        if (crossValidatedMeanOfModelAccuracy != null) {
            summary.setCrossValidatedMean(crossValidatedMeanOfModelAccuracy.asDouble());
        }

        JsonNode crossValidatedStdOfModelAccuracy = json.get("CrossValidatedStdOfModelAccuracy");
        if (crossValidatedStdOfModelAccuracy != null) {
            summary.setCrossValidatedStd(crossValidatedStdOfModelAccuracy.asDouble());
        }

        return summary;
    }

    @SuppressWarnings("unchecked")
    private void setLiftStatistics(JsonNode json, ModelSummary summary) {
        List<Map<String, ?>> segmentations = JsonUtils.getOrDefault(json, List.class, new ArrayList<>());

        if (segmentations.size() == 0) {
            return;
        }
        List<Map<String, Object>> segments = (List<Map<String, Object>>) segmentations.get(0).get("Segments");

        long cumulativeSegmentRowCountForTestSet = 0;
        long cumulativeSegmentConvertedCountForTestSet = 0;
        int index = 1;
        double conversionRateForWholeTestSet = (double) summary.getTestConversionCount()
                / (double) summary.getTestRowCount();

        List<Double> topNPctLift = new ArrayList<>();
        for (Map<String, Object> segment : segments) {
            double segmentRowCountForTestSet = Double.parseDouble(segment.get("Count").toString());
            double segmentConvertedCountForTestSet = Double.parseDouble(segment.get("Converted").toString());

            cumulativeSegmentRowCountForTestSet += segmentRowCountForTestSet;
            cumulativeSegmentConvertedCountForTestSet += segmentConvertedCountForTestSet;

            if (index % 10 == 0) {
                double cumulativeConversionRate = (double) cumulativeSegmentConvertedCountForTestSet
                        / (double) cumulativeSegmentRowCountForTestSet;
                topNPctLift.add(cumulativeConversionRate / conversionRateForWholeTestSet);
            }

            if (index == 30) {
                break;
            }
            index++;
        }
        summary.setTop10PercentLift(topNPctLift.get(0));
        summary.setTop20PercentLift(topNPctLift.get(1));
        summary.setTop30PercentLift(topNPctLift.get(2));
    }

    private List<Predictor> parsePredictors(JsonNode predictorsJsonNode, ModelSummary summary) {

        List<Predictor> predictors = new ArrayList<Predictor>();
        if (predictorsJsonNode == null) {
            log.warn("This modelsummary file does not have any predictor.");
            return predictors;
        }

        if (summary == null) {
            throw new NullPointerException("This summary should not be null.");
        }

        if (!predictorsJsonNode.isArray()) {
            throw new IllegalArgumentException("The modelsummary should be a JSON Array.");
        }
        for (final JsonNode predictorJson : predictorsJsonNode) {
            Predictor predictor = new Predictor();

            predictor.setName(JsonUtils.getOrDefault(predictorJson.get(NAME), String.class, DEFAULT_PREDICTOR_NAME));
            predictor.setDisplayName(JsonUtils.getOrDefault(predictorJson.get(PREDICTOR_DISPLAY_NAME), String.class,
                    DEFAULT_PREDICTOR_DISPLAY_NAME));
            predictor.setApprovedUsage(JsonUtils.getOrDefault(predictorJson.get(PREDICTOR_APPROVED_USAGE), String.class,
                    DEFAULT_PREDICTOR_APPROVED_USAGE));
            predictor.setCategory(JsonUtils.getOrDefault(predictorJson.get(PREDICTOR_CATEGORY), String.class,
                    DEFAULT_PREDICTOR_CATEGORY));
            predictor.setFundamentalType(JsonUtils.getOrDefault(predictorJson.get(PREDICTOR_FUNDAMENTAL_TYPE),
                    String.class, DEFAULT_PREDICTOR_FUNDAMENTAL_TYPE));
            predictor.setUncertaintyCoefficient(
                    JsonUtils.getOrDefault(predictorJson.get(PREDICTOR_UNCERTAINTY_COEFFICIENT), Double.class,
                            DEFAULT_PREDICTOR_UNCERTAINTY_COEFFICIENT));
            predictor.setModelSummary(summary);
            predictor.setTenantId(summary.getTenantId());

            predictors.add(predictor);
        }

        sortAndSetPredictors(predictors);

        return predictors;
    }

    private void sortAndSetPredictors(List<Predictor> predictors) {
        if (predictors == null) {
            throw new NullPointerException("Predictors should not be null.");
        }
        int numberUsedForBuyerInsights = predictors.size();
        if (numberUsedForBuyerInsights > defaultBiPredictorNum) {
            numberUsedForBuyerInsights = defaultBiPredictorNum;
        }
        // sort predictors according to uncertainty coefficient in descending
        // order
        Collections.sort(predictors);

        // set the top ones to be used for BuyerInsights
        for (int i = 0; i < numberUsedForBuyerInsights; i++) {
            Predictor predictor = predictors.get(i);
            predictor.setUsedForBuyerInsights(true);
        }
        return;
    }

    private String getDate(long milliSeconds, String dateFormat) {
        SimpleDateFormat formatter = new SimpleDateFormat(dateFormat);
        formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(milliSeconds);
        return formatter.format(calendar.getTime());
    }

    public String parseOriginalName(String nameDatetime) {
        String dateTimePattern = "(0[1-9]|1[012])/(0[1-9]|[12][0-9]|3[01])/(19|20)\\d\\d";
        Pattern pattern = Pattern.compile(dateTimePattern);
        Matcher matcher = pattern.matcher(nameDatetime);
        if (matcher.find()) {
            return nameDatetime.substring(0, matcher.start() - 1);
        } else {
            return nameDatetime;
        }
    }

    public boolean isIncomplete(JsonNode summaryJson) {
        return !(summaryJson.has("Segmentations") && summaryJson.has("Predictors") && summaryJson.has("ModelDetails")
                && summaryJson.has("TopSample") && summaryJson.has("BottomSample")
                && summaryJson.has("EventTableProvenance"));
    }
}

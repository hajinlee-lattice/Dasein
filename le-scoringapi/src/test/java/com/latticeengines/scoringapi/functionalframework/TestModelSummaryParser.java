package com.latticeengines.scoringapi.functionalframework;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;

public class TestModelSummaryParser {

    private static final Logger log = LoggerFactory.getLogger(TestModelSummaryParser.class);

    public static final String NAME = "Name";

    public static final String MODEL_SUMMARY_PREDICTORS = "Predictors";

    public static final String MODEL_SUMMARY_SEGMENTATIONS = "Segmentations";

    public static final String DEFAULT_PREDICTOR_NAME = "DefaultPredictorName";

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

    private int defaultBiPredictorNum = 10;

    public void setPredictors(ModelSummary summary, String modelSummaryJsonLocalResourcePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json;
        InputStream modelSummaryFileAsStream = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(modelSummaryJsonLocalResourcePath);
        String fileContents = new String(IOUtils.toByteArray(modelSummaryFileAsStream));
        modelSummaryFileAsStream.close();
        json = mapper.readValue(fileContents, JsonNode.class);

        // parse summary
        JsonNode summaryJsonNode = json.get("Summary");

        // parse predictors
        JsonNode predictorsJsonNode = summaryJsonNode.get(MODEL_SUMMARY_PREDICTORS);
        List<Predictor> predictors = parsePredictors(predictorsJsonNode, summary);
        summary.setPredictors(predictors);
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

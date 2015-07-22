package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class ModelAlerts {

    private ModelQualityWarnings modelQualityWarnings;
    private MissingMetaDataWarnings missingMetaDataWarnings;

    @JsonProperty("ModelQualityWarnings")
    public ModelQualityWarnings getModelQualityWarnings() {
        return modelQualityWarnings;
    }

    @JsonProperty("ModelQualityWarnings")
    public void setModelQualityWarnings(ModelQualityWarnings modelQualityWarnings) {
        this.modelQualityWarnings = modelQualityWarnings;
    }

    @JsonProperty("MissingMetaDataWarnings")
    public MissingMetaDataWarnings getMissingMetaDataWarnings() {
        return missingMetaDataWarnings;
    }

    @JsonProperty("MissingMetaDataWarnings")
    public void setMissingMetaDataWarnings(MissingMetaDataWarnings missingMetaDataWarnings) {
        this.missingMetaDataWarnings = missingMetaDataWarnings;
    }

    public String toString() {
        return JsonUtils.serialize(this);
    }

    @JsonInclude(Include.NON_NULL)
    public class ModelQualityWarnings {

        private Long lowSuccessEvents;
        private Long minSuccessEvents;
        private Double lowConversionPercentage; // if conversion rate is 0.8%, this value will be 0.8
        private Double minConversionPercentage;
        private Double outOfRangeRocScore;
        private Double minRocScore;
        private Double maxRocScore;
        private List<Map.Entry<String, String>> excessiveDiscreteValuesAttributes = new ArrayList<>();
        private Long maxNumberOfDiscreteValues;
        private List<Map.Entry<String, String>> excessivePredictiveAttributes = new ArrayList<>();
        private Double maxFeatureImportance;
        private List<Map.Entry<String, String>> excessivePredictiveNullValuesAttributes = new ArrayList<>();
        private Double maxLiftForNull;

        @JsonProperty("LowSuccessEvents")
        public Long getLowSuccessEvents() {
            return lowSuccessEvents;
        }

        @JsonProperty("LowSuccessEvents")
        public void setLowSuccessEvents(Long lowSuccessEvents) {
            this.lowSuccessEvents = lowSuccessEvents;
        }

        @JsonProperty("MinSuccessEvents")
        public Long getMinSuccessEvents() {
            return minSuccessEvents;
        }

        @JsonProperty("MinSuccessEvents")
        public void setMinSuccessEvents(Long minSuccessEvents) {
            this.minSuccessEvents = minSuccessEvents;
        }

        @JsonProperty("LowConversionPercentage")
        public Double getLowConversionPercentage() {
            return lowConversionPercentage;
        }

        @JsonProperty("LowConversionPercentage")
        public void setLowConversionPercentage(Double lowConversionPercentage) {
            this.lowConversionPercentage = lowConversionPercentage;
        }

        @JsonProperty("MinConversionPercentage")
        public Double getMinConversionPercentage() {
            return minConversionPercentage;
        }

        @JsonProperty("MinConversionPercentage")
        public void setMinConversionPercentage(Double minConversionPercentage) {
            this.minConversionPercentage = minConversionPercentage;
        }

        @JsonProperty("OutOfRangeRocScore")
        public Double getOutOfRangeRocScore() {
            return outOfRangeRocScore;
        }

        @JsonProperty("OutOfRangeRocScore")
        public void setOutOfRangeRocScore(Double outOfRangeRocScore) {
            this.outOfRangeRocScore = outOfRangeRocScore;
        }

        @JsonProperty("MinRocScore")
        public Double getMinRocScore() {
            return minRocScore;
        }

        @JsonProperty("MinRocScore")
        public void setMinRocScore(Double minRocScore) {
            this.minRocScore = minRocScore;
        }

        @JsonProperty("MaxRocScore")
        public Double getMaxRocScore() {
            return maxRocScore;
        }

        @JsonProperty("MaxRocScore")
        public void setMaxRocScore(Double maxRocScore) {
            this.maxRocScore = maxRocScore;
        }

        @JsonProperty("ExcessiveDiscreteValuesAttributes")
        public List<Map.Entry<String, String>> getExcessiveDiscreteValuesAttributes() {
            return excessiveDiscreteValuesAttributes;
        }

        @JsonProperty("ExcessiveDiscreteValuesAttributes")
        public void setExcessiveDiscreteValuesAttributes(
                List<Map.Entry<String, String>> excessiveDiscreteValuesAttributes) {
            this.excessiveDiscreteValuesAttributes = excessiveDiscreteValuesAttributes;
        }

        @JsonProperty("MaxNumberOfDiscreteValues")
        public Long getMaxNumberOfDiscreteValues() {
            return maxNumberOfDiscreteValues;
        }

        @JsonProperty("MaxNumberOfDiscreteValues")
        public void setMaxNumberOfDiscreteValues(Long maxNumberOfDiscreteValues) {
            this.maxNumberOfDiscreteValues = maxNumberOfDiscreteValues;
        }

        @JsonProperty("ExcessivePredictiveAttributes")
        public List<Map.Entry<String, String>> getExcessivePredictiveAttributes() {
            return excessivePredictiveAttributes;
        }

        @JsonProperty("ExcessivePredictiveAttributes")
        public void setExcessivePredictiveAttributes(
                List<Map.Entry<String, String>> excessivePredictiveAttributes) {
            this.excessivePredictiveAttributes = excessivePredictiveAttributes;
        }

        @JsonProperty("MaxFeatureImportance")
        public Double getMaxFeatureImportance() {
            return maxFeatureImportance;
        }

        @JsonProperty("MaxFeatureImportance")
        public void setMaxFeatureImportance(Double maxFeatureImportance) {
            this.maxFeatureImportance = maxFeatureImportance;
        }

        @JsonProperty("ExcessivePredictiveNullValuesAttributes")
        public List<Map.Entry<String, String>> getExcessivePredictiveNullValuesAttributes() {
            return excessivePredictiveNullValuesAttributes;
        }

        @JsonProperty("ExcessivePredictiveNullValuesAttributes")
        public void setExcessivePredictiveNullValuesAttributes(
                List<Map.Entry<String, String>> excessivePredictiveNullValuesAttributes) {
            this.excessivePredictiveNullValuesAttributes = excessivePredictiveNullValuesAttributes;
        }

        @JsonProperty("MaxLiftForNull")
        public Double getMaxLiftForNull() {
            return maxLiftForNull;
        }

        @JsonProperty("MaxLiftForNull")
        public void setMaxLiftForNull(Double maxLiftForNull) {
            this.maxLiftForNull = maxLiftForNull;
        }
    }

    @JsonInclude(Include.NON_NULL)
    public class MissingMetaDataWarnings {

        private List<String> invalidApprovedUsageAttributes = new ArrayList<>();
        private List<String> invalidTagsAttributes = new ArrayList<>();
        private List<String> invalidCategoryAttributes = new ArrayList<>();
        private List<String> invalidDisplayNameAttributes = new ArrayList<>();
        private List<String> invalidStatisticalTypeAttributes = new ArrayList<>();
        private List<String> excessiveCategoriesInModelSummary = new ArrayList<>();
        private Long maxCategoriesInModelSummary;

        @JsonProperty("InvalidApprovedUsageAttributes")
        public List<String> getInvalidApprovedUsageAttributes() {
            return invalidApprovedUsageAttributes;
        }

        @JsonProperty("InvalidApprovedUsageAttributes")
        public void setInvalidApprovedUsageMissingAttributes(
                List<String> invalidApprovedUsageAttributes) {
            this.invalidApprovedUsageAttributes = invalidApprovedUsageAttributes;
        }

        @JsonProperty("InvalidTagsAttributes")
        public List<String> getInvalidTagsAttributes() {
            return invalidTagsAttributes;
        }

        @JsonProperty("InvalidTagsAttributes")
        public void setInvalidTagsAttributes(List<String> invalidTagsAttributes) {
            this.invalidTagsAttributes = invalidTagsAttributes;
        }

        @JsonProperty("InvalidCategoryAttributes")
        public List<String> getInvalidCategoryAttributes() {
            return invalidCategoryAttributes;
        }

        @JsonProperty("InvalidCategoryAttributes")
        public void setInvalidCategoryAttributes(List<String> invalidCategoryAttributes) {
            this.invalidCategoryAttributes = invalidCategoryAttributes;
        }

        @JsonProperty("InvalidDisplayNameAttributes")
        public List<String> getInvalidDisplayNameAttributes() {
            return invalidDisplayNameAttributes;
        }

        @JsonProperty("InvalidDisplayNameAttributes")
        public void setInvalidDisplayNameAttributes(
                List<String> invalidDisplayNameAttributes) {
            this.invalidDisplayNameAttributes = invalidDisplayNameAttributes;
        }

        @JsonProperty("InvalidStatisticalTypeAttributes")
        public List<String> getInvalidStatisticalTypeAttributes() {
            return invalidStatisticalTypeAttributes;
        }

        @JsonProperty("InvalidStatisticalTypeAttributes")
        public void setInvalidStatisticalTypeAttributes(
                List<String> invalidStatisticalTypeAttributes) {
            this.invalidStatisticalTypeAttributes = invalidStatisticalTypeAttributes;
        }

        @JsonProperty("ExcessiveCategoriesInModelSummary")
        public List<String> getExcessiveCategoriesInModelSummary() {
            return excessiveCategoriesInModelSummary;
        }

        @JsonProperty("ExcessiveCategoriesInModelSummary")
        public void setExcessiveCategoriesInModelSummary(List<String> excessiveCategoriesInModelSummary) {
            this.excessiveCategoriesInModelSummary = excessiveCategoriesInModelSummary;
        }

        @JsonProperty("MaxCategoriesInModelSummary")
        public Long getMaxCategoriesInModelSummary() {
            return maxCategoriesInModelSummary;
        }

        @JsonProperty("MaxCategoriesInModelSummary")
        public void setMaxCategoriesInModelSummary(Long maxCategoriesInModelSummary) {
            this.maxCategoriesInModelSummary = maxCategoriesInModelSummary;
        }

    }

}

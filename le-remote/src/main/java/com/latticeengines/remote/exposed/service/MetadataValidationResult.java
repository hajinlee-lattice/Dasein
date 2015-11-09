package com.latticeengines.remote.exposed.service;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.exception.AnnotationValidationError;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MetadataValidationResult {

    @JsonProperty("ApprovedUsageAnnotationErrors")
    private List<Map.Entry<String, AnnotationValidationError>> approvedUsageAnnotationErrors;
    @JsonProperty("TagsAnnotationErrors")
    private List<Map.Entry<String, AnnotationValidationError>> tagsAnnotationErrors;
    @JsonProperty("CategoryAnnotationErrors")
    private List<Map.Entry<String, AnnotationValidationError>> categoryAnnotationErrors;
    @JsonProperty("DisplayNameAnnotationErrors")
    private List<Map.Entry<String, AnnotationValidationError>> displayNameAnnotationErrors;
    @JsonProperty("StatisticalTypeAnnotationErrors")
    private List<Map.Entry<String, AnnotationValidationError>> statisticalTypeAnnotationErrors;

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public boolean isCorrect() {
        return ((this.approvedUsageAnnotationErrors == null || this.approvedUsageAnnotationErrors.size() == 0)
                && (this.tagsAnnotationErrors == null || this.tagsAnnotationErrors.size() == 0)
                && (this.categoryAnnotationErrors == null || this.categoryAnnotationErrors.size() == 0)
                && (this.displayNameAnnotationErrors == null || this.displayNameAnnotationErrors.size() == 0) && (this.statisticalTypeAnnotationErrors == null || this.statisticalTypeAnnotationErrors
                .size() == 0));
    }

    public List<Map.Entry<String, AnnotationValidationError>> getApprovedUsageAnnotationErrors() {
        return this.approvedUsageAnnotationErrors;
    }

    public List<Map.Entry<String, AnnotationValidationError>> getTagsAnnotationErrors() {
        return this.tagsAnnotationErrors;
    }

    public List<Map.Entry<String, AnnotationValidationError>> getCategoryAnnotationErrors() {
        return this.categoryAnnotationErrors;
    }

    public List<Map.Entry<String, AnnotationValidationError>> getDisplayNameAnnotationErrors() {
        return this.displayNameAnnotationErrors;
    }

    public List<Map.Entry<String, AnnotationValidationError>> getStatisticalTypeAnnotationErrors() {
        return this.statisticalTypeAnnotationErrors;
    }

    public static class Builder {

        private List<Map.Entry<String, AnnotationValidationError>> approvedUsageAnnotationErrors = null;
        private List<Map.Entry<String, AnnotationValidationError>> tagsAnnotationErrors = null;
        private List<Map.Entry<String, AnnotationValidationError>> categoryAnnotationErrors = null;
        private List<Map.Entry<String, AnnotationValidationError>> displayNameAnnotationErrors = null;
        private List<Map.Entry<String, AnnotationValidationError>> statisticalTypeAnnotationErrors = null;

        public Builder approvedUsageAnnotationErrors(List<Map.Entry<String, AnnotationValidationError>> val) {
            approvedUsageAnnotationErrors = val;
            return this;
        }

        public Builder tagsAnnotationErrors(List<Map.Entry<String, AnnotationValidationError>> val) {
            tagsAnnotationErrors = val;
            return this;
        }

        public Builder categoryAnnotationErrors(List<Map.Entry<String, AnnotationValidationError>> val) {
            categoryAnnotationErrors = val;
            return this;
        }

        public Builder displayNameAnnotationErrors(List<Map.Entry<String, AnnotationValidationError>> val) {
            displayNameAnnotationErrors = val;
            return this;
        }

        public Builder statisticalTypeAnnotationErrors(List<Map.Entry<String, AnnotationValidationError>> val) {
            statisticalTypeAnnotationErrors = val;
            return this;
        }

        public MetadataValidationResult build() {
            return new MetadataValidationResult(this);
        }
    }

    private MetadataValidationResult(Builder builder) {
        this.approvedUsageAnnotationErrors = builder.approvedUsageAnnotationErrors;
        this.categoryAnnotationErrors = builder.categoryAnnotationErrors;
        this.displayNameAnnotationErrors = builder.displayNameAnnotationErrors;
        this.statisticalTypeAnnotationErrors = builder.statisticalTypeAnnotationErrors;
        this.tagsAnnotationErrors = builder.tagsAnnotationErrors;
    }

    public static void main(String[] args) {
        List<Map.Entry<String, AnnotationValidationError>> approvedUsageAnnotationErrors = new ArrayList<Map.Entry<String, AnnotationValidationError>>();
        Map.Entry<String, AnnotationValidationError> entry = new AbstractMap.SimpleEntry<String, AnnotationValidationError>(
                "key", null);
        approvedUsageAnnotationErrors.add(entry);
        MetadataValidationResult result = new MetadataValidationResult.Builder().approvedUsageAnnotationErrors(
                approvedUsageAnnotationErrors).build();
        System.out.println(result);
    }
}

package com.latticeengines.ulysses.monitoring;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

public class UlyssesRecMetrics implements Dimension, Fact {
    private Integer getRecommendationDurationMS;

    private String recommendationId;

    @MetricTag(tag = "RecommendationId")
    public String getRecId() {
        return recommendationId;
    }

    public void setRecId(String recommendationId) {
        this.recommendationId = recommendationId;
    }

    @MetricField(name = "GetRecommendationDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getRecommendationDurationMS() {
        return getRecommendationDurationMS;
    }

    public void setGetRecommendationDurationMS(int getRecommendationDurationMS) {
        this.getRecommendationDurationMS = getRecommendationDurationMS;
    }

}

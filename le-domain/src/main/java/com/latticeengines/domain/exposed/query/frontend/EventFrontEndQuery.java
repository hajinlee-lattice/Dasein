package com.latticeengines.domain.exposed.query.frontend;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.KryoUtils;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.query.Query;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class EventFrontEndQuery extends FrontEndQuery {
    @JsonProperty(FrontEndQueryConstants.SEGMENT_QUERY)
    private FrontEndQuery segmentQuery;

    @JsonIgnore
    private Query segmentSubQuery;

    @JsonProperty(FrontEndQueryConstants.PERIOD_NAME)
    private String periodName;

    @JsonProperty(FrontEndQueryConstants.PERIOD_COUNT)
    private int periodCount = -1;

    @JsonProperty(FrontEndQueryConstants.TARGET_PRODUCT_IDS)
    private List<String> targetProductIds;

    @JsonProperty(FrontEndQueryConstants.CALCULATE_PRODUCT_REVENUE)
    private boolean calculateProductRevenue = false;

    @JsonProperty(FrontEndQueryConstants.EVALUATION_PERIOD_ID)
    private int evaluationPeriodId = -1;

    @JsonProperty(FrontEndQueryConstants.LAGGING_PERIOD_COUNT)
    private int laggingPeriodCount = 1;

    public static EventFrontEndQuery fromFrontEndQuery(FrontEndQuery frontEndQuery) {
        return JsonUtils.deserialize(JsonUtils.serialize(frontEndQuery), EventFrontEndQuery.class);
    }

    public static EventFrontEndQuery fromSegment(MetadataSegment metadataSegment) {
        return fromFrontEndQuery(FrontEndQuery.fromSegment(metadataSegment));
    }

    public FrontEndQuery getSegmentQuery() {
        return segmentQuery;
    }

    public void setSegmentQuery(FrontEndQuery segmentQuery) {
        this.segmentQuery = segmentQuery;
    }

    public String getPeriodName() {
        return periodName;
    }

    public void setPeriodName(String periodName) {
        this.periodName = periodName;
    }

    public int getPeriodCount() {
        return periodCount;
    }

    public void setPeriodCount(int periodCount) {
        this.periodCount = periodCount;
    }

    public int getLaggingPeriodCount() {
        return laggingPeriodCount;
    }

    public void setLaggingPeriodCount(int laggingPeriodCount) {
        this.laggingPeriodCount = laggingPeriodCount;
    }

    public int getEvaluationPeriodId() {
        return evaluationPeriodId;
    }

    public void setEvaluationPeriodId(int evaluationPeriodId) {
        this.evaluationPeriodId = evaluationPeriodId;
    }

    public boolean getCalculateProductRevenue() {
        return calculateProductRevenue;
    }

    public void setCalculateProductRevenue(boolean calculateProductRevenue) {
        this.calculateProductRevenue = calculateProductRevenue;
    }

    public List<String> getTargetProductIds() {
        return targetProductIds;
    }

    public void setTargetProductIds(List<String> targetProductIds) {
        this.targetProductIds = targetProductIds;
    }

    public Query getSegmentSubQuery() {
        return segmentSubQuery;
    }

    public void setSegmentSubQuery(Query segmentSubQuery) {
        this.segmentSubQuery = segmentSubQuery;
    }

    public EventFrontEndQuery getDeepCopy() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        KryoUtils.write(bos, this);
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        return KryoUtils.read(bis, EventFrontEndQuery.class);
    }
}

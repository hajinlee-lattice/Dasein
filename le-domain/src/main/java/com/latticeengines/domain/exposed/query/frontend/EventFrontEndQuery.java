package com.latticeengines.domain.exposed.query.frontend;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class EventFrontEndQuery extends FrontEndQuery {

    @JsonProperty(FrontEndQueryConstants.PERIOD_NAME)
    private String periodName;

    @JsonProperty(FrontEndQueryConstants.PERIOD_COUNT)
    private int periodCount = -1;

    @JsonProperty(FrontEndQueryConstants.TARGET_PRODUCT_IDS)
    private List<String> targetProductIds;

    @JsonProperty(FrontEndQueryConstants.CALCULATE_PRODUCT_REVENUE)
    private boolean calculateProductRevenue = false;

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

    public static EventFrontEndQuery fromFrontEndQuery(FrontEndQuery frontEndQuery) {
        return JsonUtils.deserialize(JsonUtils.serialize(frontEndQuery), EventFrontEndQuery.class);
    }

    public static EventFrontEndQuery fromSegment(MetadataSegment metadataSegment) {
        return fromFrontEndQuery(FrontEndQuery.fromSegment(metadataSegment));
    }
}

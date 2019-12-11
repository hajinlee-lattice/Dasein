package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
        @Type(value = RatingEngineActionConfiguration.class, name = "ratingEngineActionConfiguration"),
        @Type(value = SegmentActionConfiguration.class, name = "segmentActionConfiguration"),
        @Type(value = ActivityMetricsActionConfiguration.class, name = "activityMetricsActionConfiguration"),
        @Type(value = AttrConfigLifeCycleChangeConfiguration.class, name = "attrConfigLifeCycleChangeConfiguration"),
        @Type(value = ImportActionConfiguration.class, name = "importActionConfiguration"),
        @Type(value = DeleteActionConfiguration.class, name = "deleteActionConfiguration"),
        @Type(value = CleanupActionConfiguration.class, name = "cleanupActionConfiguration"),
        @Type(value = LegacyDeleteActionConfiguration.class, name = "legacyDeleteActionConfiguration") })
public abstract class ActionConfiguration {

    private Boolean hiddenFromUI = false;

    public abstract String serialize();

    public Boolean isHiddenFromUI() {
        return hiddenFromUI;
    }

    public void setHiddenFromUI(Boolean hiddenFromUI) {
        this.hiddenFromUI = hiddenFromUI;
    }
}

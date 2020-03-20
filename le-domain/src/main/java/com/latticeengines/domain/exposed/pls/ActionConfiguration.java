package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = As.WRAPPER_OBJECT, property = "property")
@JsonSubTypes({ //
        @Type(value = RatingEngineActionConfiguration.class, name = "ratingEngineActionConfiguration"),
        @Type(value = SegmentActionConfiguration.class, name = "segmentActionConfiguration"),
        @Type(value = ActivityMetricsActionConfiguration.class, name = "activityMetricsActionConfiguration"),
        @Type(value = AttrConfigLifeCycleChangeConfiguration.class, name = "attrConfigLifeCycleChangeConfiguration"),
        @Type(value = ImportActionConfiguration.class, name = "importActionConfiguration"),
        @Type(value = DeleteActionConfiguration.class, name = "deleteActionConfiguration"),
        @Type(value = CleanupActionConfiguration.class, name = "cleanupActionConfiguration"),
        @Type(value = LegacyDeleteByUploadActionConfiguration.class, name = "legacyDeleteActionConfiguration"),
        @Type(value = LegacyDeleteByDateRangeActionConfiguration.class, name =
                "legacyDeleteByDataRangeActionConfiguration")
})
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

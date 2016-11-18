package com.latticeengines.domain.exposed.actors;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.common.exposed.util.MetricUtils;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;

public class VisitingHistory extends BaseMeasurement<VisitingHistory, VisitingHistory>
        implements Fact, Dimension, Measurement<VisitingHistory, VisitingHistory> {

    private final String site;
    private final Long duration;
    private final String travelerId;
    private Boolean rejected = false;
    private static final Set<String> excludedSystemTags = Collections.singleton(MetricUtils.TAG_HOST);

    public VisitingHistory(String travelerId, String site, Long duration) {
        this.travelerId = travelerId;
        this.site = site;
        this.duration = duration;
    }

    @MetricTag(tag = "Site")
    public String getSite() {
        return site;
    }

    public Long getDuration() {
        return duration;
    }

    @MetricField(name = "Duration", fieldType = MetricField.FieldType.DOUBLE)
    public Double getDurationAsDouble() {
        return duration.doubleValue();
    }

    @MetricField(name = "TravelerId", fieldType = MetricField.FieldType.STRING)
    public String getTravelerId() {
        return travelerId;
    }

    @MetricField(name = "Rejected", fieldType = MetricField.FieldType.BOOLEAN)
    public Boolean getRejected() {
        return rejected;
    }

    public void setRejected(Boolean rejected) {
        this.rejected = rejected;
    }

    @Override
    public VisitingHistory getFact() {
        return this;
    }

    @Override
    public VisitingHistory getDimension() {
        return this;
    }

    @Override
    public RetentionPolicy getRetentionPolicy() {
        return RetentionPolicyImpl.ONE_WEEK;
    }

    @Override
    public Collection<String> excludeSystemTags() {
        return excludedSystemTags;
    }

}

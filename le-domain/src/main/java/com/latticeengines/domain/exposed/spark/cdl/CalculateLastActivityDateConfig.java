package com.latticeengines.domain.exposed.spark.cdl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CalculateLastActivityDateConfig extends SparkJobConfig {

    public static final String NAME = "calLastActivityDate";
    private static final long serialVersionUID = -761308206490827854L;

    // indices of account stream tables in input
    @JsonProperty
    public List<Integer> accountStreamInputIndices = new ArrayList<>();

    // indices of contact stream tables in input
    @JsonProperty
    public List<Integer> contactStreamInputIndices = new ArrayList<>();

    // index of contact table in input, use to retrieve contact -> account mapping
    @JsonProperty
    public Integer contactTableIdx;

    // list of date attribute name, in the same indices as corresponding input
    // e.g., dateAttrs(1) is the name of event time attribute of input(1)
    @JsonProperty
    public List<String> dateAttrs = new ArrayList<>();

    // discard events that has records earlier than this time, null to keep all
    // events
    @JsonProperty
    public Long earliestEventTimeInMillis;

    @Override
    public int getNumTargets() {
        if (CollectionUtils.isNotEmpty(contactStreamInputIndices)) {
            return 2;
        } else {
            return CollectionUtils.isNotEmpty(accountStreamInputIndices) ? 1 : 0;
        }
    }

    @Override
    public String getName() {
        return NAME;
    }
}

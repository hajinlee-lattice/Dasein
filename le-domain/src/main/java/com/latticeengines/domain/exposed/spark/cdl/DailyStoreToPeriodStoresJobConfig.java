package com.latticeengines.domain.exposed.spark.cdl;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class DailyStoreToPeriodStoresJobConfig extends SparkJobConfig implements Serializable {

    private static final long serialVersionUID = 0L;

    public static final String NAME = "DailyStoreToPeriodStoresJobConfig";

    @JsonProperty("stream")
    public AtlasStream stream;

    @JsonProperty("currentDateStr")
    public String currentDateStr;

    @JsonProperty("entity")
    public BusinessEntity entity;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    // generate 4: Week, Month, Quarter, Year
    // depends on stream config. Only generate requested period stores, others will be empty dataframe
    @Override
    public int getNumTargets() {
        return stream.getPeriods().size();
    }
}

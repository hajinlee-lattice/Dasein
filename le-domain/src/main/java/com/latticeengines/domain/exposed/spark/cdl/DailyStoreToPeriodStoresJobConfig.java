package com.latticeengines.domain.exposed.spark.cdl;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class DailyStoreToPeriodStoresJobConfig extends SparkJobConfig implements Serializable {

    private static final long serialVersionUID = 0L;

    public static final String NAME = "dailyStoreToPeriodStoresJob";

    @JsonProperty("streams")
    public List<AtlasStream> streams;

    @JsonProperty("evaluationDate")
    public String evaluationDate;

    @JsonProperty("inputMetadata")
    public ActivityStoreSparkIOMetadata inputMetadata; // describes streamId -> dailyStore input index

    @JsonProperty("businessCalendar")
    public BusinessCalendar businessCalendar;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    // generate 4: Week, Month, Quarter, Year
    // depends on stream config. Only generate requested period stores, others will be empty dataframe
    @Override
    public int getNumTargets() {
        return streams.stream().mapToInt(stream -> stream.getPeriods().size()).sum();
    }
}

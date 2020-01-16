package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.common.CSVJobConfigBase;

public class MergeCSVConfig extends CSVJobConfigBase {

    private static final long serialVersionUID = 9165843065652451988L;

    public static final String NAME = "mergeCSV";

    @JsonProperty("Compress")
    private Boolean compress;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

}

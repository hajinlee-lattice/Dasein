package com.latticeengines.domain.exposed.datacloud.customer;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IncorrectLookupReproduceDetail extends ReproduceDetail{

    @Override
    @JsonProperty("ReproduceDetailType")
    public String getReproduceDetailType() {
        return this.getClass().getSimpleName();
    }

}

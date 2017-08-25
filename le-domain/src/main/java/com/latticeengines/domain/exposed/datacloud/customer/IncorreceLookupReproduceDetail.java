package com.latticeengines.domain.exposed.datacloud.customer;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IncorreceLookupReproduceDetail extends ReproduceDetail{

    @Override
    @JsonProperty("ReproduceDetailType")
    public String getReproduceDetailType() {
        return this.getClass().getSimpleName();
    }

}

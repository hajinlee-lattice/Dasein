package com.latticeengines.domain.exposed.ulysses;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

import io.swagger.annotations.ApiModelProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CompanyProfileRequest {
    @JsonProperty("record")
    @ApiModelProperty(value = "A record is represented as a JSON Object; " //
            + "ie. { \"field1\" : value1, \"field2\" : value2, .......}. " //
            + "At the minimum, in case of lead type model, make sure to specify " //
            + "'Email' field and for non-lead type model specify either 'Website' " //
            + "or 'Domain' fields. If these fields are not specified then specify " //
            + "both 'CompanyName' and 'State' fields", //
    required = true)
    private Map<String, Object> record = new HashMap<>();

    public Map<String, Object> getRecord() {
        return record;
    }

    public void setRecord(Map<String, Object> record) {
        this.record = record;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}

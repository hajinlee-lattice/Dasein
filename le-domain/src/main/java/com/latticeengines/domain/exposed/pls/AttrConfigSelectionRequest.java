package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AttrConfigSelectionRequest {

    @JsonProperty("Select")
    private List<String> select;

    @JsonProperty("Deselect")
    private List<String> deselect;

    public List<String> getSelect() {
        return this.select;
    }

    public void setSelect(List<String> select) {
        this.select = select;
    }

    public List<String> getDeselect() {
        return this.deselect;
    }

    public void setDeselect(List<String> deselect) {
        this.deselect = deselect;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}

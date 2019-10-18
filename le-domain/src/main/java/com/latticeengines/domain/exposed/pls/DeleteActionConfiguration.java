package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DeleteActionConfiguration extends ActionConfiguration {

    @JsonProperty("delete_data_table")
    private String deleteDataTable;

    public String getDeleteDataTable() {
        return deleteDataTable;
    }

    public void setDeleteDataTable(String deleteDataTable) {
        this.deleteDataTable = deleteDataTable;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Override
    public String serialize() {
        return toString();
    }
}

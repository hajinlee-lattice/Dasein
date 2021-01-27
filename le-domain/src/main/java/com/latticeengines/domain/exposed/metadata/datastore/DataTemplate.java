package com.latticeengines.domain.exposed.metadata.datastore;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.MasterSchema;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataTemplate {

    @JsonProperty("masterSchema")
    private MasterSchema masterSchema;

    @JsonProperty("tenant")
    private String tenant;

    @JsonProperty("name")
    private String name;

    public MasterSchema getMasterSchema() {
        return masterSchema;
    }

    public void setMasterSchema(MasterSchema masterSchema) {
        this.masterSchema = masterSchema;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static void main(String[] args){
        DataTemplate dataTemplate = new DataTemplate();
        System.out.println(JsonUtils.serialize(dataTemplate));

    }

}

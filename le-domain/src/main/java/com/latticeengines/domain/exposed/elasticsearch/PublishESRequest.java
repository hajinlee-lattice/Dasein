package com.latticeengines.domain.exposed.elasticsearch;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class PublishESRequest {

    @JsonProperty("TableRoles")
    private Set<TableRoleInCollection> tableRoles = new HashSet<>();
    @JsonProperty("Version")
    private DataCollection.Version version;
    //using to set elastic search property
    @JsonProperty("ESConfig")
    private ElasticSearchConfig esConfig;
    //the tenant has table role using publish to es
    @JsonProperty("OriginTenant")
    private String originTenant;


    public Set<TableRoleInCollection> getTableRoles() {
        return tableRoles;
    }

    public void setTableRoles(Set<TableRoleInCollection> tableRoles) {
        this.tableRoles = tableRoles;
    }

    public DataCollection.Version getVersion() {
        return version;
    }

    public void setVersion(DataCollection.Version version) {
        this.version = version;
    }

    public ElasticSearchConfig getEsConfig() {
        return esConfig;
    }

    public void setEsConfig(ElasticSearchConfig esConfig) {
        this.esConfig = esConfig;
    }

    public String getOriginTenant() {
        return originTenant;
    }

    public void setOriginTenant(String originTenant) {
        this.originTenant = originTenant;
    }
}

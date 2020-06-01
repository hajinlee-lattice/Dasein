package com.latticeengines.domain.exposed.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class PublishTableRoleRequest {

    @JsonProperty("TableRoles")
    private List<TableRoleInCollection> tableRoles;

    @JsonProperty("Version")
    private DataCollection.Version version;

    public List<TableRoleInCollection> getTableRoles() {
        return tableRoles;
    }

    public void setTableRoles(List<TableRoleInCollection> tableRoles) {
        this.tableRoles = tableRoles;
    }

    public DataCollection.Version getVersion() {
        return version;
    }

    public void setVersion(DataCollection.Version version) {
        this.version = version;
    }
}

package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;

public class PublishTableRoleStepConfiguration extends ImportExportS3StepConfiguration {

    @JsonProperty("table_role")
    private List<TableRoleInCollection> tableRoles;

    public List<TableRoleInCollection> getTableRoles() {
        return tableRoles;
    }

    public void setTableRoles(List<TableRoleInCollection> tableRoles) {
        this.tableRoles = tableRoles;
    }
}

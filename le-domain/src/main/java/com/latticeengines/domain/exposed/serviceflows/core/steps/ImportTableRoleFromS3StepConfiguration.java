package com.latticeengines.domain.exposed.serviceflows.core.steps;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

public class ImportTableRoleFromS3StepConfiguration extends ImportExportS3StepConfiguration {

    private List<TableRoleInCollection> tableRoleInCollections;

    public List<TableRoleInCollection> getTableRoleInCollections() {
        return tableRoleInCollections;
    }

    public void setTableRoleInCollections(List<TableRoleInCollection> tableRoleInCollections) {
        this.tableRoleInCollections = tableRoleInCollections;
    }
}

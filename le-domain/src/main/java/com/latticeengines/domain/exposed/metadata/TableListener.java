package com.latticeengines.domain.exposed.metadata;

import javax.persistence.PostLoad;
import javax.persistence.PrePersist;

public class TableListener {

    @PostLoad
    public void tablePostLoad(Table table) {
        table.setTableTypeCode(table.getTableTypeCode());
    }
    
    @PrePersist
    public void tablePrePersist(Table table) {
        for (TableTag tag : table.getTableTags()) {
            tag.setTable(table);
            tag.setTenantId(table.getTenantId());
        }
    }
}

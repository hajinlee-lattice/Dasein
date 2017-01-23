package com.latticeengines.domain.exposed.metadata;

import java.util.List;

import javax.persistence.PostLoad;
import javax.persistence.PrePersist;

import com.latticeengines.common.exposed.util.HibernateUtils;

public class TableListener {

    @PostLoad
    public void tablePostLoad(Table table) {
        table.setTableTypeCode(table.getTableTypeCode());
        HibernateUtils.inflateDetails(table.getStorageMechanisms());
    }
    
    @PrePersist
    public void tablePrePersist(Table table) {
        for (TableTag tag : table.getTableTags()) {
            tag.setTable(table);
            tag.setTenantId(table.getTenantId());
        }

        List<StorageMechanism> storageMechanisms = table.getStorageMechanisms();
        
        if (storageMechanisms.size() == 0) {
            StorageMechanism hdfs = new HdfsStorage();
            hdfs.setTableNameInStorage(table.getName());
            table.addStorageMechanism(hdfs);
        }
        
        for (StorageMechanism storageMechanism : table.getStorageMechanisms()) {
            storageMechanism.setTable(table);
        }
    }
}

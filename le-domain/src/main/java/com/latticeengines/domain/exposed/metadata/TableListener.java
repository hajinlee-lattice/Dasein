package com.latticeengines.domain.exposed.metadata;

import javax.persistence.PostLoad;
import javax.persistence.PrePersist;

import com.latticeengines.common.exposed.util.HibernateUtils;

public class TableListener {

    @PostLoad
    public void tablePostLoad(Table table) {
        table.setTableTypeCode(table.getTableTypeCode());
        HibernateUtils.inflateDetails(table.getStorageMechanism());
    }
    
    @PrePersist
    public void tablePrePersist(Table table) {
        StorageMechanism storageMechanism = table.getStorageMechanism();
        
        if (storageMechanism == null) {
            StorageMechanism hdfs = new HdfsStorage();
            hdfs.setTableNameInStorage(table.getName());
            table.setStorageMechanism(hdfs);
        }
        
        table.getStorageMechanism().setTable(table);
    }
}

package com.latticeengines.metadata.entitymgr.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.TableType;

@Component("tableTypeHolder")
public class TableTypeHolder {

    private ThreadLocal<TableType> tableTypeThreadLocal = ThreadLocal.withInitial(() -> TableType.DATATABLE);
    
    public void setTableType(TableType tableType) {
        tableTypeThreadLocal.set(tableType);
    }
    
    public TableType getTableType() {
        return tableTypeThreadLocal.get();
    }
    

}

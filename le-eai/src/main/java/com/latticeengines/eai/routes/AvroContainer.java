package com.latticeengines.eai.routes;

import com.latticeengines.domain.exposed.eai.Table;

public class AvroContainer {

    private Table table;
    
    public AvroContainer(Table table) {
        this.setTable(table);
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }
}

package com.latticeengines.domain.exposed.metadata;

import java.util.HashMap;
import java.util.Map;

public enum TableType {
    DATATABLE(0),
    IMPORTTABLE(1);
    
    private Integer code;
    
    TableType(Integer code) {
        this.code = code;
    }
    
    private static Map<Integer, TableType> tableTypeMap = new HashMap<>();
    
    static {
        for (TableType type : TableType.values()) {
            tableTypeMap.put(type.getCode(), type);
        }
    }
    
    public Integer getCode() {
        return code;
    }
    
    public static TableType getTableTypeByCode(Integer code) {
        return tableTypeMap.get(code);
    }

}

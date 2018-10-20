package com.latticeengines.domain.exposed.metadata;

import java.util.HashMap;
import java.util.Map;

public enum TableType {
    DATATABLE(0), IMPORTTABLE(1);

    private static Map<Integer, TableType> tableTypeMap = new HashMap<>();

    static {
        for (TableType type : TableType.values()) {
            tableTypeMap.put(type.getCode(), type);
        }
    }

    private Integer code;

    TableType(Integer code) {
        this.code = code;
    }

    public static TableType getTableTypeByCode(Integer code) {
        return tableTypeMap.get(code);
    }

    public Integer getCode() {
        return code;
    }

}

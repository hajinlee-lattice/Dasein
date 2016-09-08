package com.latticeengines.domain.exposed.dellebi;

import java.util.HashMap;
import java.util.Map;

public enum DellEbiExecutionLogStatus {

    NewFile(1), //
    Downloaded(2), //
    Transformed(3), //
    Exported(4), //
    Completed(5), //
    TriedFailed(-2), //
    Failed(-1);

    private int status;
    private static Map<Integer, String> enumMap;

    DellEbiExecutionLogStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return this.status;
    }

    public static String getStatusNameByCode(int code) {
        if (enumMap == null) {
            initialMap();
        }

        return enumMap.get(code);
    }

    private static void initialMap() {
        enumMap = new HashMap<Integer, String>();
        for (DellEbiExecutionLogStatus accessor : DellEbiExecutionLogStatus.values()) {
            enumMap.put(accessor.getStatus(), accessor.toString());
        }
    }
}

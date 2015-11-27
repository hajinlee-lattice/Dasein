package com.latticeengines.domain.exposed.dellebi;

public enum DellEbiExecutionLogStatus {

    NewFile(1), //
    Downloaded(2), //
    Transformed(3), //
    Exported(4), //
    Completed(5), //
    Failed(-1);

    private int status;

    DellEbiExecutionLogStatus(int status) {
        this.status = status;
    }

    public int getStatus() {
        return this.status;
    }

}

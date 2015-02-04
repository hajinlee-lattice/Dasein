package com.latticeengines.scoringharness.operationmodel;

import com.latticeengines.scoringharness.OutputFileWriter;

public abstract class Operation<T extends OperationSpec> {
    protected T spec;
    protected OutputFileWriter output;

    public Operation() {
    }

    public abstract String getName();

    public T getSpec() {
        return spec;
    }

    public void setSpec(T spec) {
        this.spec = spec;
    }

    public void setOutput(OutputFileWriter output) {
        this.output = output;
    }

    public abstract void execute();
}

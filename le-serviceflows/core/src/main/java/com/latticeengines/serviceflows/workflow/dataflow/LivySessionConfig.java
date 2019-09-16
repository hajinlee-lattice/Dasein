package com.latticeengines.serviceflows.workflow.dataflow;

public class LivySessionConfig {

    public final int scalingMulitplier;

    public final int  partitionMultiplier;

    public LivySessionConfig(int scalingMulitplier, int partitionMultiplier) {
        this.scalingMulitplier = scalingMulitplier;
        this.partitionMultiplier = partitionMultiplier;
    }

}

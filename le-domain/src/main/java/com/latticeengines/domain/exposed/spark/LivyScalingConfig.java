package com.latticeengines.domain.exposed.spark;

public class LivyScalingConfig {

    public final int scalingMultiplier;

    public final int  partitionMultiplier;

    public LivyScalingConfig(int scalingMultiplier, int partitionMultiplier) {
        this.scalingMultiplier = scalingMultiplier;
        this.partitionMultiplier = partitionMultiplier;
    }

}

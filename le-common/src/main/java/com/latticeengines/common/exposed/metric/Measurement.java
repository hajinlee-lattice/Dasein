package com.latticeengines.common.exposed.metric;

public interface Measurement<F extends Fact, D extends Dimension> {

    D getDimension();

    F getFact();

    RetentionPolicy getRetentionPolicy();

}

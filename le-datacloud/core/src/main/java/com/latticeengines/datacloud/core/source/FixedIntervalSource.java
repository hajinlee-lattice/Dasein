package com.latticeengines.datacloud.core.source;

public interface FixedIntervalSource extends DerivedSource {

    String getDirForBaseVersionLookup();

    String getTransformationServiceBeanName();

    Long getCutoffDuration();

}

package com.latticeengines.propdata.core.source;

public interface FixedIntervalSource extends DerivedSource {

    String getDirForBaseVersionLookup();

    String getTransformationServiceBeanName();

}

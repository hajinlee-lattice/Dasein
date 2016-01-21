package com.latticeengines.propdata.core.source;


public interface MostRecentSource extends DerivedSource {

    @Override
    CollectedSource[] getBaseSources();

    Long periodToKeep();

}

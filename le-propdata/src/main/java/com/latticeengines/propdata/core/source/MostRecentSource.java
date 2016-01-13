package com.latticeengines.propdata.core.source;


public interface MostRecentSource extends ServingSource {

    @Override
    CollectedSource[] getBaseSources();

    Long periodToKeep();

}

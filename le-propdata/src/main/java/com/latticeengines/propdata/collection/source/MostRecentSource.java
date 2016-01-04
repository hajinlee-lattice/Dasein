package com.latticeengines.propdata.collection.source;


public interface MostRecentSource extends ServingSource {

    @Override
    CollectedSource[] getBaseSources();

    Long periodTokeep();

}

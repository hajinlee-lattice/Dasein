package com.latticeengines.propdata.core.source;

public interface DerivedSource extends Source {

    Source[] getBaseSources();

    PurgeStrategy getPurgeStrategy();

    Integer getNumberOfVersionsToKeep();

    Integer getNumberOfDaysToKeep();

}
